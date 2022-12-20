import os
import random
import unittest

from pathlib import Path
from connection import DriveConnection


class TestStringMethods(unittest.TestCase):

    def test_upload_download(self):
        table_name = os.environ['table_name']
        sheet_name = os.environ['sheet_name']

        self.assertIsNotNone(table_name)
        self.assertIsNotNone(sheet_name)

        data_dir = Path(__file__).resolve().parent.parent / 'data'
        drive = DriveConnection(credentials_dir=data_dir / 'credentials.json', token_dir=data_dir / 'token.json')

        df = drive.download(drive_table=table_name, sheet_name=sheet_name)

        new_column_value = str(random.random())
        df['column1'] = new_column_value

        drive.upload(df, drive_table=table_name, sheet_name=sheet_name)

        df = drive.download(drive_table=table_name, sheet_name=sheet_name)

        values = df['column1'].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)


if __name__ == '__main__':
    unittest.main()
