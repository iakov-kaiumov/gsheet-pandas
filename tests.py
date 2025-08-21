import os
import random
import unittest

from pathlib import Path

import pandas as pd

from gsheet_pandas.adapter import connection
import dotenv

BASE_DIR = Path(__file__).resolve().parent

dotenv.load_dotenv(BASE_DIR / ".env")

spreadsheet_id = os.getenv("table_name")
sheet_name = os.getenv("sheet_name")

data_dir = Path(__file__).resolve().parent / "data"


class TestConnectionMethods(unittest.TestCase):
    @staticmethod
    def _get_drive() -> connection.DriveConnection:
        return connection.DriveConnection(
            credentials_dir=data_dir / "credentials.json",
            token_dir=data_dir / 'token.json'
        )

    def test_list_sheets(self):
        drive = self._get_drive()
        sheets = drive.get_sheets_names(spreadsheet_id)
        self.assertEqual(sheets, ["test", "test2"])

    def test_create_sheet(self):
        drive = self._get_drive()
        _id = drive.create_sheet(spreadsheet_id=spreadsheet_id, sheet_name="test2")
        self.assertIsNone(_id)

    def test_connection_class(self):
        drive = self._get_drive()
        df = drive.download(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        new_column_value = str(random.random())
        df["column1"] = new_column_value

        drive.upload(df, spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        df = drive.download(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        values = df["column1"].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)

    def test_pandas_extension(self):
        connection.setup(
            credentials_dir=data_dir / "credentials.json",
            token_dir=data_dir / "token.json",
        )

        df = pd.from_gsheet(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
        new_column_value = str(random.random())
        df["column1"] = new_column_value

        df.to_gsheet(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        df = pd.from_gsheet(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)
        values = df["column1"].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)


if __name__ == "__main__":
    unittest.main()
