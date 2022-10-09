import pandas as pd

from anp_sales_miner.jobs.cleaner import Cleaner


class TotalTable(Cleaner):
    def __init__(self, table_name):
        super().__init__(table_name=f'{table_name}_totals')

    def melt_table(self, df):
        id_vars = ['month']
        value_vars = set(df.columns).difference(id_vars)
        return pd.melt(df, id_vars, value_vars, var_name='year', value_name='volume')

    def clean_columns_syntax(self, df):
        df = super().clean_columns_syntax(df)
        df['volume'] = df['volume'].fillna(0.0)
        return df

    def create_columns(self, df):
        df = super().create_columns(df)
        df['uf'] = 'TODAS'
        df['product'] = 'TODOS'
        df['unit'] = 'm3'
        return df
