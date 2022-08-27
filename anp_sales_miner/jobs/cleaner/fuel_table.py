import pandas as pd

from anp_sales_miner.jobs.cleaner import Cleaner


class FuelTable(Cleaner):
    def melt_table(self, df):
        id_vars = ['product', 'year', 'region', 'uf', 'unit']
        value_vars = ['Jan', 'Fev', 'Mar', 'Abr', 'Mai', 'Jun', 'Jul', 'Ago', 'Set',
                      'Out', 'Nov', 'Dez']
        return pd.melt(df, id_vars, value_vars, var_name='month', value_name='volume')

    def clean_columns_syntax(self, df):
        super().clean_columns_syntax(df)
        df['product'] = df['product'].str.replace(' (m3)', '', regex=False)
        df['product'] = df['product'].str.replace(' )', ')', regex=False)
        return df
