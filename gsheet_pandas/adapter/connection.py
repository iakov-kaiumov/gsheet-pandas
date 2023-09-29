from __future__ import print_function

import os.path
import socket
from pathlib import Path

import pandas as pd
from pandas import Timestamp
from pandas._libs.lib import Decimal

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


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
    df = df.applymap(lambda x: str(x) if isinstance(x, Timestamp) else x)
    df = df.applymap(lambda x: float(x) if isinstance(x, Decimal) else x)
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
            print(err)
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
            print(f'get_all_files_in_folder: An error occurred - {e}')
            raise e
        return files

    def download(self, drive_table: str, sheet_name: str, range_name: str = DEFAULT_RANGE_NAME) -> pd.DataFrame:
        service = self._get_service()
        sheet = self._get_service().spreadsheets()
        result = sheet.values().get(spreadsheetId=drive_table, range=sheet_name + range_name).execute()
        values = result.get('values', [])
        service.close()
        if not values:
            raise Exception('Empty data')
        df = pd.DataFrame(values[1:])
        columns = values[0]
        if len(df.columns) > len(columns):
            columns += [f'Unknown {i}' for i in range(len(df.columns) - len(columns))]
        df.columns = columns
        return df

    def upload(self,
               df: pd.DataFrame,
               drive_table: str,
               sheet_name: str,
               range_name: str = DEFAULT_RANGE_NAME,
               drop_columns: bool = False):
        try:
            df = _fix_dtypes(df)
            values = df.T.reset_index().T.values.tolist()
            if drop_columns:
                values = df.values.tolist()
            service = self._get_service()
            service.spreadsheets().values().update(
                spreadsheetId=drive_table,
                valueInputOption='RAW',
                range=sheet_name + range_name,
                body=dict(majorDimension='ROWS', values=values),
            ).execute()
            service.close()
        except socket.timeout as e:
            print(f'pandas_to_sheet: Error: {e}')
            raise e
        except Exception as e:
            print(f'pandas_to_sheet: Error: {e}')
            raise e
