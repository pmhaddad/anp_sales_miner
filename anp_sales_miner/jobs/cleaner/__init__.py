import datetime
from abc import ABC, abstractmethod

import pandas as pd

from anp_sales_miner.helpers.io_helper import make_path, write_as_parquet


class Cleaner(ABC):
    def __init__(self, table_name):
        self.table_name = table_name

        self.rename_map = {'COMBUSTÍVEL': 'product',
                           'ANO': 'year',
                           'REGIÃO': 'region',
                           'ESTADO': 'uf',
                           'UNIDADE': 'unit'}

        self.month_map = {'Jan': '01', 'Janeiro': '01',
                          'Fev': '02', 'Fevereiro': '02',
                          'Mar': '03', 'Março': '03',
                          'Abr': '04', 'Abril': '04',
                          'Mai': '05', 'Maio': '05',
                          'Jun': '06', 'Junho': '06',
                          'Jul': '07', 'Julho': '07',
                          'Ago': '08', 'Agosto': '08',
                          'Set': '09', 'Setembro': '09',
                          'Out': '10', 'Outubro': '10',
                          'Nov': '11', 'Novembro': '11',
                          'Dez': '12', 'Dezembro': '12'}

        self.table_schema = {'year_month': 'string',
                             'uf': 'string',
                             'product': 'string',
                             'unit': 'string',
                             'volume': 'double',
                             'created_at': 'datetime64[ns]'}

    def read_parsed_table(self):
        return pd.read_csv(make_path(stage='parsed', table=f'{self.table_name}.csv'),
                           engine='python')

    def rename_columns(self, df):
        return df.rename(columns=self.rename_map, errors='ignore')

    @abstractmethod
    def melt_table(self):
        raise NotImplementedError('This method must be overwritten by a child class!')

    def clean_columns_syntax(self, df):
        df['year'] = df['year'].astype(str).str.replace(r'\.0$', '', regex=True)
        df['month'] = df['month'].map(self.month_map)
        return df

    def create_columns(self, df):
        df['year_month'] = df['year'] + '-' + df['month']
        df['created_at'] = datetime.datetime.now()
        return df

    def set_schema(self, df):
        return df[[*self.table_schema.keys()]].astype(self.table_schema)

    def run(self):
        df = self.read_parsed_table()
        df = self.rename_columns(df)
        df = self.melt_table(df)
        df = self.clean_columns_syntax(df)
        df = self.create_columns(df)
        df = self.set_schema(df)

        write_as_parquet(df,
                         output_path=make_path(stage='clean', table=self.table_name),
                         partitioning=['product', 'year_month'])
