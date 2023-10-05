import os
from pathlib import Path

import pandas as pd
from ydata_profiling import ProfileReport

from anp_sales_miner.helpers.io_helper import make_path


def _make_output_path(base_dir='reports', filename='anp_fuel_sales.html'):
    Path(base_dir).mkdir(parents=False, exist_ok=True)
    return os.sep.join([base_dir, filename])


def profile_table():
    transformed_df = pd.read_parquet(make_path(stage='transformed', table='anp_fuel_sales'))

    # Columns partitioned with pyarrow or fastparquet do not store data types. See:
    # https://stackoverflow.com/questions/57308349/datatypes-are-not-preserved-when-a-pandas-dataframe-partitioned-and-saved-as-par
    transformed_df['year_month'] = pd.to_datetime(transformed_df['year_month'], format=r'%Y-%m')
    profile = ProfileReport(transformed_df, title='ANP Fuel Sales Profiling Report')
    profile.to_file(_make_output_path())
