import datetime

import pandas as pd
from pandas import Timestamp
from pandas._libs.lib import Decimal


def _fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix data types in DataFrame for proper upload to Google Sheets.

    :param df: DataFrame to process
    :return: Processed DataFrame
    """
    df = df.fillna("")
    df = df.map(
        lambda x: str(x)
        if isinstance(x, (Timestamp, datetime.datetime, datetime.date))
        else x
    )
    df = df.map(lambda x: float(x) if isinstance(x, Decimal) else x)
    return df


def _escape_sheet_name(sheet_name: str) -> str:
    if sheet_name.startswith("'") and sheet_name.endswith("'"):
        return sheet_name

    sheet_name = sheet_name.replace("'", "''")
    return f"'{sheet_name}'"
