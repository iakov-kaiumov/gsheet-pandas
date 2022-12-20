# gsheet-pandas

Package gheet-pandas lets you to easily get Pandas dataframe from Google Sheets.

## Usage
First, init DriveConnection instance:
```python
from gsheet-pandas import DriveConnection
drive = DriveConnection(credentials_dir='credentials.json', token_dir='token.json')
```

To download dataframe:
```python
df = drive.download(drive_table=table_name, sheet_name=sheet_name)
```

To upload dataframe:
```python
drive.upload(df, drive_table=table_name, sheet_name=sheet_name)
```

## Installation
Install using pip:
```
pip install gsheet-pandas
```
