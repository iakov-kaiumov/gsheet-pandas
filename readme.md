# Google sheet & Pandas intergation

[![PyPI Latest Release](https://img.shields.io/pypi/v/gsheet-pandas.svg)](https://pypi.org/project/gsheet-pandas/)
[![License](https://img.shields.io/pypi/l/gsheet-pandas.svg)](https://github.com/iakov-kaiumov/gsheet-pandas/blob/main/LICENSE)

Package `gheet-pandas` allows you to easily get Pandas dataframe from Google Sheets or upload dataframe to the Sheets.

## Installation
Install using pip:
```
pip install gsheet-pandas
```

## Set up environment
### Enable the API

Before using Google APIs, you need to turn them on in a Google Cloud project. You can turn on one or more APIs in a single Google Cloud project.
In the Google Cloud console, enable the Google Sheets API.

Enable the API

### Authorize credentials for a desktop application

To authenticate as an end user and access user data in your app, you need to create one or more OAuth 2.0 Client IDs. A client ID is used to identify a single app to Google's OAuth servers. If your app runs on multiple platforms, you must create a separate client ID for each platform.
1. In the Google Cloud console, go to **Menu > APIs & Services > Credentials**.
2. Go to **Credentials**
3. Click **Create Credentials > OAuth client ID**. 
4. Click **Application type > Desktop app**. 
5. In the **Name** field, type a name for the credential. This name is only shown in the Google Cloud console. 
6. Click **Create**. The OAuth client created screen appears, showing your new Client ID and Client secret. 
7. Click **OK**. The newly created credential appears under **OAuth 2.0 Client IDs**. 
8. Save the downloaded JSON file as `credentials.json`, and move the file to your working directory.

## Usage
First, init DriveConnection instance:
```python
from gsheet_pandas import DriveConnection
secret_path = Path('/path/to/my/secrets/').resolve()
drive = DriveConnection(credentials_dir=secret_path / 'credentials.json', 
                        token_dir=secret_path / 'token.json')
```

To download dataframe:
```python
df = drive.download(drive_table=table_name, sheet_name=sheet_name)
```

To upload dataframe:
```python
drive.upload(df, drive_table=table_name, sheet_name=sheet_name)
```
