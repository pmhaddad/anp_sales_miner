import pandas as pd

from anp_sales_miner.helpers.io_helper import make_path


def validate_table(table_name, precision=5):
    processed_data = _read_table(table_name)
    calculated_totals = _calculate_totals(processed_data)

    original_totals = _read_table(table_name, suffix='_totals')

    check_join = pd.merge(calculated_totals, original_totals, how='outer', on=['year_month'],
                          indicator=True, suffixes=('_calculated', '_original'))

    assert (check_join['_merge'] == 'both').all(), _get_error('mismatched')

    rounded_calculated_volumes = check_join['volume_calculated'].round(precision)
    rounded_original_volumes = check_join['volume_original'].round(precision)
    assert (rounded_calculated_volumes == rounded_original_volumes).all(), _get_error('unequal')


def _read_table(table_name, suffix=''):
    return pd.read_parquet(make_path(stage='clean', table=f'{table_name}{suffix}'))


def _calculate_totals(df):
    return df.groupby('year_month')['volume'].sum()


def _get_error(error_key):
    validation_errors = {
        'mismatched': 'There are mismatched records between processed and original totals!',
        'unequal': 'There are unequal values for totals between processed and original totals!'
    }
    return validation_errors.get(error_key)
