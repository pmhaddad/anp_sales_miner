import pandas as pd

from anp_sales_miner.helpers.io_helper import make_path, write_as_parquet


class Transformer():
    def __init__(self, table_name):
        self.table_name = table_name

    def run(self):
        df = pd.read_parquet(make_path(stage='clean', table=self.table_name))

        write_as_parquet(df,
                         output_path=make_path(stage='transformed', table='anp_fuel_sales'),
                         partitioning=['product', 'year_month'])
