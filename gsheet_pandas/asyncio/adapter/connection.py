import datetime
import json
import logging
import os.path
from pathlib import Path
from typing import Literal, Optional

import pandas as pd
from pandas import Timestamp
from pandas._libs.lib import Decimal

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds, UserCreds

logger = logging.getLogger("gsheet-pandas")

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
DEFAULT_RANGE_NAME = "!A1:ZZ900000"

async_drive_connection = None


async def setup(credentials_dir: Path, token_dir: Path = None):
    """
    Initialize async connection to Google Drive and register pandas extensions.

    :param credentials_dir: Path to credentials.json file
    :param token_dir: Path to token.json file (optional)
    """
    global async_drive_connection
    async_drive_connection = AsyncDriveConnection(credentials_dir, token_dir)

    def inner_generator():
        async def inner(df, *args, **kwargs):
            await async_drive_connection.upload(df, *args, **kwargs)

        return inner

    import pandas

    pandas.DataFrame.to_gsheet_async = inner_generator()
    pandas.from_gsheet_async = async_drive_connection.download


def _fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix data types in DataFrame for proper upload to Google Sheets.

    :param df: DataFrame to process
    :return: Processed DataFrame
    """
    df = df.fillna("")
    df = df.map(
        lambda x: str(x)
        if isinstance(x, (Timestamp, datetime.datetime, datetime.date))
        else x
    )
    df = df.map(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df


class AsyncDriveConnection:
    """
    Async class for working with Google Drive and Google Sheets.
    """

    def __init__(self, credentials_dir: Path, token_dir: Path = None):
        """
        Initialize async connection.

        :param credentials_dir: Path to credentials.json or service account file
        :param token_dir: Path to token.json file (optional, only for OAuth2)
        """
        self.credentials_dir = credentials_dir
        self.token_dir = token_dir
        self._aiogoogle = None
        self._creds = None

    def _get_user_creds_sync(self):
        """
        Synchronously get user OAuth2 credentials (for initial setup).
        """
        creds = None
        if os.path.exists(self.token_dir):
            creds = Credentials.from_authorized_user_file(
                self.token_dir.__str__(), SCOPES
            )
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.credentials_dir.__str__(), SCOPES
                )
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_dir, "w") as token:
                token.write(creds.to_json())
        return creds

    async def _get_aiogoogle(self):
        """
        Get or create Aiogoogle instance with authorization.
        """
        if self._aiogoogle is None:
            if self.token_dir is None:
                # Service account
                with open(self.credentials_dir.__str__()) as f:
                    creds_dict = json.load(f)
                self._creds = ServiceAccountCreds(scopes=SCOPES, **creds_dict)
                self._aiogoogle = Aiogoogle(service_account_creds=self._creds)
            else:
                # User OAuth2
                user_creds = self._get_user_creds_sync()
                creds_dict = {
                    "access_token": user_creds.token,
                    "refresh_token": user_creds.refresh_token,
                    "expires_at": user_creds.expiry.isoformat()
                    if user_creds.expiry
                    else None,
                    "scopes": SCOPES,
                }
                self._creds = UserCreds(**creds_dict)
                self._aiogoogle = Aiogoogle(user_creds=self._creds)

        return self._aiogoogle

    async def get_all_files_in_folder(self, folder_id):
        """
        Asynchronously get all files in specified Google Drive folder.

        :param folder_id: Google Drive folder ID
        :return: List of files
        """
        files = []
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                drive_v3 = await aiogoogle.discover("drive", "v3")
                page_token = None

                while True:
                    if self.token_dir is None:
                        # Service account
                        response = await aiogoogle.as_service_account(
                            drive_v3.files.list(
                                q=f"'{folder_id}' in parents",
                                pageSize=100,
                                fields="nextPageToken, files(id, name)",
                                pageToken=page_token,
                            )
                        )
                    else:
                        # User OAuth2
                        response = await aiogoogle.as_user(
                            drive_v3.files.list(
                                q=f"'{folder_id}' in parents",
                                pageSize=100,
                                fields="nextPageToken, files(id, name)",
                                pageToken=page_token,
                            )
                        )

                    files.extend(response.get("files", []))
                    page_token = response.get("nextPageToken", None)
                    if page_token is None:
                        break

        except Exception as e:
            raise e
        return files

    async def download(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        range_name: str = DEFAULT_RANGE_NAME,
        header: Optional[int] = 0,
    ) -> pd.DataFrame:
        """
        Asynchronously download Google Spreadsheet as Pandas DataFrame.

        :param spreadsheet_id: Spreadsheet ID
        :param sheet_name: Sheet name
        :param range_name: Cell range (default !A1:ZZ900000)
        :param header: Header row index
        :return: DataFrame
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover("sheets", "v4")

                if self.token_dir is None:
                    # Service account
                    result = await aiogoogle.as_service_account(
                        sheets_v4.spreadsheets.values.get(
                            spreadsheetId=spreadsheet_id, range=sheet_name + range_name
                        )
                    )
                else:
                    # User OAuth2
                    result = await aiogoogle.as_user(
                        sheets_v4.spreadsheets.values.get(
                            spreadsheetId=spreadsheet_id, range=sheet_name + range_name
                        )
                    )

                values = result.get("values", [])

                if not values:
                    raise Exception("Empty data")

                if header is None:
                    return pd.DataFrame(values)

                columns = values[header]
                data = values[header + 1 :]
                if len(data) == 0:
                    # Return empty df
                    return pd.DataFrame(columns=columns)

                df = pd.DataFrame(data)
                if len(df.columns) > len(columns):
                    columns += [
                        f"Unknown {i}" for i in range(len(df.columns) - len(columns))
                    ]
                df.columns = columns
                return df

        except Exception as e:
            logger.error(f"Error downloading spreadsheet: {e}")
            raise e

    async def upload(
        self,
        df: pd.DataFrame,
        spreadsheet_id: str,
        sheet_name: str,
        range_name: str = DEFAULT_RANGE_NAME,
        drop_columns: bool = False,
        value_input_option: Literal[
            "INPUT_VALUE_OPTION_UNSPECIFIED", "RAW", "USER_ENTERED"
        ] = "RAW",
    ) -> None:
        """
        Asynchronously upload Pandas DataFrame to Google Spreadsheet.

        :param df: Pandas DataFrame
        :param spreadsheet_id: Spreadsheet ID
        :param sheet_name: Sheet name
        :param range_name: Cell range (default !A1:ZZ900000)
        :param drop_columns: Whether to drop column headers
        :param value_input_option: Value input option (see Google Sheets API documentation)
        """
        try:
            df = _fix_dtypes(df)
            values = df.T.reset_index().T.values.tolist()
            if drop_columns:
                values = df.values.tolist()

            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover("sheets", "v4")

                body = {"majorDimension": "ROWS", "values": values}

                request = sheets_v4.spreadsheets.values.clear(
                    spreadsheetId=spreadsheet_id,
                    range=sheet_name + range_name,
                )

                if self.token_dir is None:
                    # Service account
                    await aiogoogle.as_service_account(request)
                else:
                    # User OAuth2
                    await aiogoogle.as_user(request)

                request = sheets_v4.spreadsheets.values.update(
                    spreadsheetId=spreadsheet_id,
                    range=sheet_name + range_name,
                    valueInputOption=value_input_option,
                    json=body,
                )

                if self.token_dir is None:
                    # Service account
                    await aiogoogle.as_service_account(request)
                else:
                    # User OAuth2
                    await aiogoogle.as_user(request)

        except Exception as e:
            logger.error(f"Error uploading to spreadsheet: {e}")
            raise e

    async def get_sheets_names(self, spreadsheet_id: str) -> list[str]:
        """
        Asynchronously get sheet names in spreadsheet.

        :param spreadsheet_id: Spreadsheet ID
        :return: List of sheet names
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover("sheets", "v4")

                if self.token_dir is None:
                    # Service account
                    sheet_metadata = await aiogoogle.as_service_account(
                        sheets_v4.spreadsheets.get(spreadsheetId=spreadsheet_id)
                    )
                else:
                    # User OAuth2
                    sheet_metadata = await aiogoogle.as_user(
                        sheets_v4.spreadsheets.get(spreadsheetId=spreadsheet_id)
                    )

                sheets = sheet_metadata.get("sheets", "")
                return [sheet.get("properties", {}).get("title") for sheet in sheets]

        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            raise e

    async def create_sheet(self, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
        """
        Asynchronously create new sheet in existing spreadsheet.

        :param spreadsheet_id: Spreadsheet ID
        :param sheet_name: New sheet name
        :return: New sheet ID if successful, None if sheet already exists
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover("sheets", "v4")

                batch_update_spreadsheet_request_body = {
                    "requests": [
                        {
                            "addSheet": {
                                "properties": {
                                    "title": sheet_name,
                                }
                            }
                        }
                    ]
                }

                try:
                    if self.token_dir is None:
                        # Service account
                        response = await aiogoogle.as_service_account(
                            sheets_v4.spreadsheets.batchUpdate(
                                spreadsheetId=spreadsheet_id,
                                json=batch_update_spreadsheet_request_body,
                            )
                        )
                    else:
                        # User OAuth2
                        response = await aiogoogle.as_user(
                            sheets_v4.spreadsheets.batchUpdate(
                                spreadsheetId=spreadsheet_id,
                                json=batch_update_spreadsheet_request_body,
                            )
                        )

                    return response["replies"][0]["addSheet"]["properties"]["sheetId"]

                except Exception as e:
                    if (
                        hasattr(e, "res")
                        and hasattr(e.res, "status_code")
                        and e.res.status_code == 400
                    ):
                        return None
                    raise e

        except Exception as e:
            logger.error(f"Error creating sheet: {e}")
            raise e
