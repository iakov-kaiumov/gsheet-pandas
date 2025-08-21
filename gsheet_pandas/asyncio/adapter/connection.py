import asyncio
import datetime
import logging
import os.path
from pathlib import Path
from typing import Literal

import pandas as pd
from pandas import Timestamp
from pandas._libs.lib import Decimal

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import aiohttp
from aiogoogle import Aiogoogle
from aiogoogle.auth.creds import ServiceAccountCreds, UserCreds

logger = logging.getLogger('gsheet-pandas')

SCOPES = ['https://www.googleapis.com/auth/spreadsheets', 'https://www.googleapis.com/auth/drive']
DEFAULT_RANGE_NAME = '!A1:ZZ900000'

async_drive_connection = None


async def setup(credentials_dir: Path, token_dir: Path = None):
    """
    Инициализирует асинхронное подключение к Google Drive и регистрирует pandas расширения.
    
    :param credentials_dir: Путь к файлу credentials.json
    :param token_dir: Путь к файлу token.json (опционально)
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
    Исправляет типы данных в DataFrame для корректной загрузки в Google Sheets.
    
    :param df: DataFrame для обработки
    :return: Обработанный DataFrame
    """
    df = df.fillna('')
    df = df.map(lambda x: str(x) if isinstance(x, (Timestamp, datetime.datetime, datetime.date)) else x)
    df = df.map(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df


class AsyncDriveConnection:
    """
    Асинхронный класс для работы с Google Drive и Google Sheets.
    """
    
    def __init__(self, credentials_dir: Path, token_dir: Path = None):
        """
        Инициализирует асинхронное подключение.
        
        :param credentials_dir: Путь к файлу credentials.json или service account файлу
        :param token_dir: Путь к файлу token.json (опционально, только для OAuth2)
        """
        self.credentials_dir = credentials_dir
        self.token_dir = token_dir
        self._aiogoogle = None
        self._creds = None
    
    def _get_user_creds_sync(self):
        """
        Синхронно получает пользовательские OAuth2 креды (для начальной настройки).
        """
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
    
    async def _get_aiogoogle(self):
        """
        Получает или создает экземпляр Aiogoogle с авторизацией.
        """
        if self._aiogoogle is None:
            if self.token_dir is None:
                # Service account
                service_account_creds = service_account.Credentials.from_service_account_file(
                    self.credentials_dir.__str__(), 
                    scopes=SCOPES
                )
                creds_dict = {
                    'type': 'service_account',
                    'client_email': service_account_creds.service_account_email,
                    'private_key': service_account_creds._private_key_pkcs8_pem.decode() if isinstance(
                        service_account_creds._private_key_pkcs8_pem, bytes
                    ) else service_account_creds._private_key_pkcs8_pem,
                    'private_key_id': service_account_creds._private_key_id,
                    'client_id': service_account_creds._client_id,
                    'token_uri': service_account_creds._token_uri,
                    'project_id': service_account_creds._project_id,
                    'scopes': SCOPES
                }
                self._creds = ServiceAccountCreds(**creds_dict)
                self._aiogoogle = Aiogoogle(service_account_creds=self._creds)
            else:
                # User OAuth2
                user_creds = self._get_user_creds_sync()
                creds_dict = {
                    'access_token': user_creds.token,
                    'refresh_token': user_creds.refresh_token,
                    'expires_at': user_creds.expiry.isoformat() if user_creds.expiry else None,
                    'scopes': SCOPES
                }
                self._creds = UserCreds(**creds_dict)
                self._aiogoogle = Aiogoogle(user_creds=self._creds)
        
        return self._aiogoogle
    
    async def get_all_files_in_folder(self, folder_id):
        """
        Асинхронно получает все файлы в указанной папке Google Drive.
        
        :param folder_id: ID папки в Google Drive
        :return: Список файлов
        """
        files = []
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                drive_v3 = await aiogoogle.discover('drive', 'v3')
                page_token = None
                
                while True:
                    if self.token_dir is None:
                        # Service account
                        response = await aiogoogle.as_service_account(
                            drive_v3.files.list(
                                q=f"'{folder_id}' in parents",
                                pageSize=100,
                                fields="nextPageToken, files(id, name)",
                                pageToken=page_token
                            )
                        )
                    else:
                        # User OAuth2
                        response = await aiogoogle.as_user(
                            drive_v3.files.list(
                                q=f"'{folder_id}' in parents",
                                pageSize=100,
                                fields="nextPageToken, files(id, name)",
                                pageToken=page_token
                            )
                        )
                    
                    files.extend(response.get('files', []))
                    page_token = response.get('nextPageToken', None)
                    if page_token is None:
                        break
                        
        except Exception as e:
            raise e
        return files
    
    async def download(self,
                      spreadsheet_id: str,
                      sheet_name: str,
                      range_name: str = DEFAULT_RANGE_NAME,
                      header: int | None = 0) -> pd.DataFrame:
        """
        Асинхронно загружает Google Spreadsheet как Pandas DataFrame.
        
        :param spreadsheet_id: ID таблицы
        :param sheet_name: Имя листа
        :param range_name: Диапазон ячеек (по умолчанию !A1:ZZ900000)
        :param header: Индекс строки заголовка
        :return: DataFrame
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover('sheets', 'v4')
                
                if self.token_dir is None:
                    # Service account
                    result = await aiogoogle.as_service_account(
                        sheets_v4.spreadsheets.values.get(
                            spreadsheetId=spreadsheet_id,
                            range=sheet_name + range_name
                        )
                    )
                else:
                    # User OAuth2
                    result = await aiogoogle.as_user(
                        sheets_v4.spreadsheets.values.get(
                            spreadsheetId=spreadsheet_id,
                            range=sheet_name + range_name
                        )
                    )
                
                values = result.get('values', [])
                
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
                
        except Exception as e:
            logger.error(f"Error downloading spreadsheet: {e}")
            raise e
    
    async def upload(self,
                    df: pd.DataFrame,
                    spreadsheet_id: str,
                    sheet_name: str,
                    range_name: str = DEFAULT_RANGE_NAME,
                    drop_columns: bool = False,
                    value_input_option: Literal['INPUT_VALUE_OPTION_UNSPECIFIED', 'RAW', 'USER_ENTERED'] = 'RAW') -> None:
        """
        Асинхронно загружает Pandas DataFrame в Google Spreadsheet.
        
        :param df: Pandas DataFrame
        :param spreadsheet_id: ID таблицы
        :param sheet_name: Имя листа
        :param range_name: Диапазон ячеек (по умолчанию !A1:ZZ900000)
        :param drop_columns: Удалять ли заголовки столбцов
        :param value_input_option: Опция ввода значений (см. документацию Google Sheets API)
        """
        try:
            df = _fix_dtypes(df)
            values = df.T.reset_index().T.values.tolist()
            if drop_columns:
                values = df.values.tolist()
            
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover('sheets', 'v4')
                
                body = {
                    'majorDimension': 'ROWS',
                    'values': values
                }
                
                if self.token_dir is None:
                    # Service account
                    await aiogoogle.as_service_account(
                        sheets_v4.spreadsheets.values.update(
                            spreadsheetId=spreadsheet_id,
                            range=sheet_name + range_name,
                            valueInputOption=value_input_option,
                            body=body
                        )
                    )
                else:
                    # User OAuth2
                    await aiogoogle.as_user(
                        sheets_v4.spreadsheets.values.update(
                            spreadsheetId=spreadsheet_id,
                            range=sheet_name + range_name,
                            valueInputOption=value_input_option,
                            body=body
                        )
                    )
                    
        except Exception as e:
            logger.error(f"Error uploading to spreadsheet: {e}")
            raise e
    
    async def get_sheets_names(self, spreadsheet_id: str) -> list[str]:
        """
        Асинхронно получает имена листов в таблице.
        
        :param spreadsheet_id: ID таблицы
        :return: Список имен листов
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover('sheets', 'v4')
                
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
                
                sheets = sheet_metadata.get('sheets', '')
                return [sheet.get("properties", {}).get("title") for sheet in sheets]
                
        except Exception as e:
            logger.error(f"Error getting sheet names: {e}")
            raise e
    
    async def create_sheet(self, spreadsheet_id: str, sheet_name: str) -> int | None:
        """
        Асинхронно создает новый лист в существующей таблице.
        
        :param spreadsheet_id: ID таблицы
        :param sheet_name: Имя нового листа
        :return: ID нового листа в случае успеха, None если лист уже существует
        """
        try:
            async with await self._get_aiogoogle() as aiogoogle:
                sheets_v4 = await aiogoogle.discover('sheets', 'v4')
                
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
                                body=batch_update_spreadsheet_request_body
                            )
                        )
                    else:
                        # User OAuth2
                        response = await aiogoogle.as_user(
                            sheets_v4.spreadsheets.batchUpdate(
                                spreadsheetId=spreadsheet_id,
                                body=batch_update_spreadsheet_request_body
                            )
                        )
                    
                    return response['replies'][0]['addSheet']['properties']['sheetId']
                    
                except Exception as e:
                    if hasattr(e, 'res') and hasattr(e.res, 'status_code') and e.res.status_code == 400:
                        return None
                    raise e
                    
        except Exception as e:
            logger.error(f"Error creating sheet: {e}")
            raise e
