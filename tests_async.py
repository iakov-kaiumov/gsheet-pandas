"""
Async tests for gsheet-pandas.
"""

import asyncio
import os
import random
import unittest
from pathlib import Path

import pandas as pd
import dotenv

from gsheet_pandas.asyncio.adapter import connection

BASE_DIR = Path(__file__).resolve().parent
dotenv.load_dotenv(BASE_DIR / ".env")

spreadsheet_id = os.getenv("table_name")
sheet_name = os.getenv("sheet_name")

data_dir = Path(__file__).resolve().parent / "data"
credentials_dir = data_dir / "credentials.json"
token_dir = data_dir / "token.json"
if not token_dir.exists():
    # To allow testing with service account
    token_dir = None


class TestAsyncConnectionMethods(unittest.IsolatedAsyncioTestCase):
    """
    Tests for async Google Sheets connection methods.
    """

    @staticmethod
    def _get_drive() -> connection.AsyncDriveConnection:
        """
        Create AsyncDriveConnection instance for tests.
        """
        return connection.AsyncDriveConnection(
            credentials_dir=credentials_dir, token_dir=token_dir
        )

    async def test_list_sheets(self):
        """
        Test getting list of sheets in spreadsheet.
        """
        drive = self._get_drive()
        sheets = await drive.get_sheets_names(spreadsheet_id)
        self.assertEqual(sheets, ["test", "test2"])

    async def test_create_sheet(self):
        """
        Test creating new sheet in spreadsheet.
        """
        drive = self._get_drive()
        _id = await drive.create_sheet(
            spreadsheet_id=spreadsheet_id, sheet_name="test2"
        )
        self.assertIsNone(_id)  # Sheet already exists

    async def test_connection_class(self):
        """
        Test downloading and uploading data through AsyncDriveConnection class.
        """
        drive = self._get_drive()
        df = await drive.download(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        new_column_value = str(random.random())
        df["column1"] = new_column_value

        await drive.upload(df, spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        df = await drive.download(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        values = df["column1"].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)

    async def test_pandas_extension(self):
        """
        Test pandas extensions for async operations.
        """
        await connection.setup(credentials_dir=credentials_dir, token_dir=token_dir)

        df = await pd.from_gsheet_async(
            spreadsheet_id=spreadsheet_id, sheet_name=sheet_name
        )
        new_column_value = str(random.random())
        df["column1"] = new_column_value

        await df.to_gsheet_async(spreadsheet_id=spreadsheet_id, sheet_name=sheet_name)

        df = await pd.from_gsheet_async(
            spreadsheet_id=spreadsheet_id, sheet_name=sheet_name
        )
        values = df["column1"].values.tolist()
        for value in values:
            self.assertEqual(value, new_column_value)

    async def test_parallel_downloads(self):
        """
        Test parallel download of multiple sheets.
        """
        drive = self._get_drive()

        # Parallel download of multiple sheets
        sheets_to_download = ["test", "test2"]

        tasks = [
            drive.download(spreadsheet_id=spreadsheet_id, sheet_name=sheet)
            for sheet in sheets_to_download
        ]

        results = await asyncio.gather(*tasks)

        self.assertEqual(len(results), 2)
        for df in results:
            self.assertIsInstance(df, pd.DataFrame)

    async def test_upload_with_value_input_option(self):
        """
        Test uploading data with USER_ENTERED option for formula interpretation.
        """
        drive = self._get_drive()

        # Create DataFrame with formula
        df = pd.DataFrame(
            {"A": [1, 2, 3], "B": [4, 5, 6], "Formula": ["=A2+B2", "=A3+B3", "=A4+B4"]}
        )

        # Upload with USER_ENTERED option
        await drive.upload(
            df,
            spreadsheet_id=spreadsheet_id,
            sheet_name="test2",
            value_input_option="USER_ENTERED",
        )

        # Check that data was uploaded
        downloaded_df = await drive.download(
            spreadsheet_id=spreadsheet_id, sheet_name="test2"
        )

        self.assertIsNotNone(downloaded_df)
        self.assertEqual(len(downloaded_df.columns), 3)

    async def test_error_handling(self):
        """
        Test error handling with invalid parameters.
        """
        drive = self._get_drive()

        # Try to download non-existent spreadsheet
        with self.assertRaises(Exception):
            await drive.download(spreadsheet_id="invalid_id", sheet_name="nonexistent")

    async def test_empty_dataframe(self):
        """
        Test working with empty DataFrame.
        """
        drive = self._get_drive()

        # Create empty DataFrame with headers only
        empty_df = pd.DataFrame(columns=["Col1", "Col2", "Col3"])

        # Upload empty DataFrame
        await drive.upload(
            empty_df,
            spreadsheet_id=spreadsheet_id,
            sheet_name="test2",
            drop_columns=False,
        )

        # Download back and check
        downloaded_df = await drive.download(
            spreadsheet_id=spreadsheet_id, sheet_name="test2", header=0
        )

        self.assertEqual(list(downloaded_df.columns), ["Col1", "Col2", "Col3"])
        self.assertEqual(len(downloaded_df), 0)


if __name__ == "__main__":
    unittest.main()
