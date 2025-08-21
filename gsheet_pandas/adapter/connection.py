import datetime
import logging
import os.path
import socket
from pathlib import Path
from typing import Literal, Optional

import googleapiclient
import pandas as pd
from pandas import Timestamp
from pandas._libs.lib import Decimal

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

logger = logging.getLogger('gsheet-pandas')
timeout_in_sec = 60 * 1
socket.setdefaulttimeout(timeout_in_sec)


SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DEFAULT_RANGE_NAME = '!A1:ZZ900000'

drive_connection = None


def setup(credentials_dir: Path, token_dir: Path = None):
    global drive_connection
    drive_connection = DriveConnection(credentials_dir, token_dir)

    def inner_generator():
        def inner(df, *args, **kwargs):
            drive_connection.upload(df, *args, **kwargs)
        return inner

    import pandas
    pandas.DataFrame.to_gsheet = inner_generator()
    pandas.from_gsheet = drive_connection.download


def _fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.fillna('')
    df = df.map(lambda x: str(x) if isinstance(x, (Timestamp, datetime.datetime, datetime.date)) else x)
    df = df.map(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df


class DriveConnection:
    def __init__(self, credentials_dir: Path, token_dir: Path = None):
        self.credentials_dir = credentials_dir
        self.token_dir = token_dir

    def _get_user_creds(self):
        creds = None
        if os.path.exists(self.token_dir):
            creds = Credentials.from_authorized_user_file(self.token_dir.__str__(), SCOPES)
        # If there are no (valid) credentials available, let the user log in.
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_dir.__str__(), SCOPES)
                creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(self.token_dir, 'w') as token:
                token.write(creds.to_json())
        return creds

    def _get_service_creds(self):
        return service_account.Credentials.from_service_account_file(self.credentials_dir.__str__(), scopes=SCOPES)

    def _get_service(self, service_name='sheets', service_version='v4'):
        if self.token_dir is None:
            creds = self._get_service_creds()
        else:
            creds = self._get_user_creds()

        try:
            service = build(service_name, service_version, credentials=creds)
            return service
        except HttpError as err:
            logger.error(err)
            return None

    def get_all_files_in_folder(self, folder_id):
        files = []
        try:
            page_token = None
            while True:
                response = self._get_service().files().list(
                    q=f"'{folder_id}' in parents",
                    pageSize=100,
                    fields="nextPageToken, files(id, name)",
                    pageToken=page_token
                ).execute()

                files.extend(response.get('files', []))
                page_token = response.get('nextPageToken', None)
                if page_token is None:
                    break

        except HttpError as e:
            raise e
        return files

    def download(self,
                 spreadsheet_id: str,
                 sheet_name: str,
                 range_name: str = DEFAULT_RANGE_NAME,
                 header: Optional[int] = 0) -> pd.DataFrame:
        """
        Downloads Google Spreadsheet as Pandas DataFrame
        :param spreadsheet_id: spreadsheet id
        :param sheet_name: sheet name
        :param range_name: range name (default is !A1:ZZ900000)
        :param header: index of header row
        :return dataframe
        """
        service = self._get_service()
        sheet = self._get_service().spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id, range=sheet_name + range_name).execute()
        values = result.get('values', [])
        service.close()
        if not values:
            raise Exception('Empty data')

        if header is None:
            return pd.DataFrame(values)

        columns = values[header]
        data = values[header + 1:]
        if len(data) == 0:
            # Return empty df
            return pd.DataFrame(columns=columns)

        df = pd.DataFrame(data)
        if len(df.columns) > len(columns):
            columns += [f'Unknown {i}' for i in range(len(df.columns) - len(columns))]
        df.columns = columns
        return df

    def upload(self,
               df: pd.DataFrame,
               spreadsheet_id: str,
               sheet_name: str,
               range_name: str = DEFAULT_RANGE_NAME,
               drop_columns: bool = False,
               value_input_option: Literal['INPUT_VALUE_OPTION_UNSPECIFIED', 'RAW', 'USER_ENTERED'] = 'RAW') -> None:
        """
        Uploads Pandas DataFrame to the Google Spreadsheet
        :param df: Pandas DataFrame
        :param spreadsheet_id: spreadsheet id
        :param sheet_name: sheet name
        :param range_name: range name (default is !A1:ZZ900000)
        :param drop_columns: whether to drop DataFrame columns or not
        :param value_input_option: The input value option. See here https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption
        """
        try:
            df = _fix_dtypes(df)
            values = df.T.reset_index().T.values.tolist()
            if drop_columns:
                values = df.values.tolist()
            service = self._get_service()
            service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                valueInputOption=value_input_option,
                range=sheet_name + range_name,
                body=dict(majorDimension='ROWS', values=values),
            ).execute()
            service.close()
        except socket.timeout as e:
            raise e
        except Exception as e:
            raise e

    def get_sheets_names(self, spreadsheet_id: str) -> list[str]:
        """
        Get sheets names for spreadsheet
        :param spreadsheet_id: spreadsheet id
        :return: list of names
        """
        service = self._get_service()
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', '')
        return [sheet.get("properties", {}).get("title") for sheet in sheets]

    def create_sheet(self, spreadsheet_id: str, sheet_name: str) -> Optional[int]:
        """
        Creates new sheet in existing spreadsheet
        :param spreadsheet_id: spreadsheet id
        :param sheet_name: new sheet's name
        :return: new sheet id if success, None if sheet exists
        """
        service = self._get_service()
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

        request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id,
                                                     body=batch_update_spreadsheet_request_body)
        try:
            response = request.execute()
        except googleapiclient.errors.HttpError as e:
            if e.status_code == 400:
                return None
            raise e
        service.close()
        return response['replies'][0]['addSheet']['properties']['sheetId']
