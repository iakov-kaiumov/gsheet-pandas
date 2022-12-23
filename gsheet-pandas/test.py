import os
import random
import unittest

from pathlib import Path

import pandas as pd

import connection


class TestConnectionMethods(unittest.TestCase):

    def test_connection_class(self):
        table_name = os.environ['table_name']
        sheet_name = os.environ['sheet_name']

        data_dir = Path(__file__).resolve().parent.parent / 'data'
        drive = connection.DriveConnection(credentials_dir=data_dir / 'credentials.json', token_dir=data_dir / 'token.json')

        df = drive.download(drive_table=table_name, sheet_name=sheet_name)

        new_column_value = str(random.random())
        df['column1'] = new_column_value

        drive.upload(df, drive_table=table_name, sheet_name=sheet_name)

        df = drive.download(drive_table=table_name, sheet_name=sheet_name)

        values = df['column1'].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)

    def test_pandas_extension(self):
        table_name = os.environ['table_name']
        sheet_name = os.environ['sheet_name']

        data_dir = Path(__file__).resolve().parent.parent / 'data'

        connection.setup(credentials_dir=data_dir / 'credentials.json', token_dir=data_dir / 'token.json')

        df = pd.from_gsheet(drive_table=table_name, sheet_name=sheet_name)
        new_column_value = str(random.random())
        df['column1'] = new_column_value

        df.to_gsheet(drive_table=table_name, sheet_name=sheet_name)

        df = pd.from_gsheet(drive_table=table_name, sheet_name=sheet_name)
        values = df['column1'].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)


if __name__ == '__main__':
    unittest.main()
