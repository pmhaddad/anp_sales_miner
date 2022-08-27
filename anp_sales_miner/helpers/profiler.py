import pandas as pd
from pandas_profiling import ProfileReport

from anp_sales_miner.helpers.io_helper import make_path


def profile_table():
    transformed_df = pd.read_parquet(make_path(stage='transformed', table='anp_fuel_sales'))

    # Columns partitioned with pyarrow or fastparquet do not store data types. See:
    # https://stackoverflow.com/questions/57308349/datatypes-are-not-preserved-when-a-pandas-dataframe-partitioned-and-saved-as-par
    transformed_df['year_month'] = transformed_df['year_month'].astype(str).astype('datetime64[M]')

    profile = ProfileReport(transformed_df, title='ANP Fuel Sales Profiling Report')
    profile.to_file('reports/anp_fuel_sales.html')
