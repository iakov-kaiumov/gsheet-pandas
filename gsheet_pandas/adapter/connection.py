from __future__ import print_function

import os.path
import socket
import time
from pathlib import Path

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

timeout_in_sec = 60 * 1  # 3 minutes timeout limit
socket.setdefaulttimeout(timeout_in_sec)


# If modifying these scopes, delete the file token.json.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DEFAULT_RANGE_NAME = '!A1:ZZ900000'

drive_connection = None


def setup(credentials_dir: Path, token_dir: Path):
    global drive_connection
    drive_connection = DriveConnection(credentials_dir=credentials_dir, token_dir=token_dir)

    def inner_generator():
        def inner(df, *args, **kwargs):
            drive_connection.upload(df, *args, **kwargs)
        return inner

    import pandas
    pandas.DataFrame.to_gsheet = inner_generator()
    pandas.from_gsheet = drive_connection.download


class DriveConnection:
    def __init__(self, credentials_dir: Path, token_dir: Path):
        self.credentials_dir = credentials_dir
        self.token_dir = token_dir

    def _get_service(self, service_name='sheets', service_version='v4'):
        creds = None
        # The file token.json stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
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

        except HttpError as error:
            print(f'get_all_files_in_folder: An error occurred - {error}')
        return files

    def download(self, drive_table: str, sheet_name: str, range_name: str = DEFAULT_RANGE_NAME) -> pd.DataFrame:
        service = self._get_service()
        sheet = self._get_service().spreadsheets()
        result = sheet.values().get(spreadsheetId=drive_table, range=sheet_name + range_name).execute()
        values = result.get('values', [])
        service.close()
        if not values:
            raise Exception('Empty data')
        return pd.DataFrame(values[1:], columns=values[0])

    def upload(self,
               df: pd.DataFrame,
               drive_table: str,
               sheet_name: str,
               range_name: str = DEFAULT_RANGE_NAME,
               drop_columns: bool = False):
        try:
            values = df.T.reset_index().T.values.tolist()
            if drop_columns:
                values = df.values.tolist()
            service = self._get_service()
            response = service.spreadsheets().values().update(
                spreadsheetId=drive_table,
                valueInputOption='RAW',
                range=sheet_name + range_name,
                body=dict(majorDimension='ROWS', values=values),
            ).execute()
            service.close()
        except socket.timeout as e:
            print(f'pandas_to_sheet: Error: {e}')
            time.sleep(1)
            self.upload(df, drive_table, sheet_name, range_name)
        except Exception as e:
            print(f'pandas_to_sheet: Error: {e}')
