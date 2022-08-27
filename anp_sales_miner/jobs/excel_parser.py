import numpy as np
import pandas as pd
from openpyxl import load_workbook
from openpyxl.pivot.fields import Missing

from anp_sales_miner.helpers.io_helper import make_path


class ExcelParser():
    def __init__(self, pivot_name, table_name, sheetname='Plan1'):
        self.pivot_name = pivot_name
        self.table_name = table_name
        self.sheetname = sheetname

    def get_pivot_table(self, worksheet):
        return [p for p in worksheet._pivots if p.name == self.pivot_name][0]

    def extract_pivot_cache(self, pivot_table):
        # Compile dict of pivot table fields that have shared items and a list of their values
        # example output: {'YEAR': [2010, 2011, 2012], 'FUEL': ['DIESEL', 'GASOLINE']}
        fields_map = {}
        for field in pivot_table.cache.cacheFields:
            if field.sharedItems.count > 0:
                fields_map[field.name] = [f.v for f in field.sharedItems._fields]

        # Extract all rows from cache records. Each row is initially parsed as a dict
        column_names = [field.name for field in pivot_table.cache.cacheFields]
        rows = []
        for record in pivot_table.cache.records.r:
            record_values = [
                field.v if not isinstance(field, Missing) else np.nan for field in record._fields
            ]

            row_dict = {k: v for k, v in zip(column_names, record_values)}

            # Shared fields are mapped as an Index, so we replace the field index by its value
            for key in fields_map:
                row_dict[key] = fields_map[key][row_dict[key]]

            rows.append(row_dict)

        return pd.DataFrame.from_dict(rows)

    def extract_pivot_totals(self, worksheet, pivot_table):
        def get_range_values(data_range):
            table = []
            for element in data_range:
                row = []
                table.append(row)
                for column in element:
                    row.append(column.value)
            return table

        pivot_data = get_range_values(data_range=worksheet[pivot_table.location.ref])

        totals_df = pd.DataFrame(data=pivot_data[pivot_table.location.firstDataRow:],
                                 columns=pivot_data[pivot_table.location.firstHeaderRow])

        return totals_df.rename(columns={'Dados': 'month'}) \
                        .query('month != "Total do Ano"')

    def run(self):
        workbook = load_workbook(make_path(stage='raw', table='vendas-combustiveis-m3.xlsx'))

        worksheet = workbook[self.sheetname]

        pivot_table = self.get_pivot_table(worksheet)

        parsed_cache_df = self.extract_pivot_cache(pivot_table)
        parsed_cache_df.to_csv(make_path(stage='parsed', table=f'{self.table_name}.csv'),
                               index=False)

        parsed_totals_df = self.extract_pivot_totals(worksheet, pivot_table)
        parsed_totals_df.to_csv(make_path(stage='parsed', table=f'{self.table_name}_totals.csv'),
                                index=False)
