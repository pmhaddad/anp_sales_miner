from unittest.mock import call, patch

import numpy as np
import pandas as pd
import pytest

from anp_sales_miner.helpers.io_helper import make_path
from anp_sales_miner.helpers.validator import (_calculate_totals, _get_error, _read_table,
                                               validate_table)

FIXTURE_ROOT = '/var/anp_sales_miner/tests/fixtures'


@pytest.fixture
def fuel_table():
    return pd.read_csv(f'{FIXTURE_ROOT}/jobs/clean_fuel_table.csv')


@pytest.fixture
def fuel_table_totals():
    return pd.read_csv(f'{FIXTURE_ROOT}/jobs/clean_fuel_table_totals.csv')


@patch('anp_sales_miner.helpers.validator._read_table')
def test_validate_table(mock__read_table, fuel_table, fuel_table_totals):
    mock__read_table.side_effect = [fuel_table, fuel_table_totals]
    validate_table('fuel_table')

    mock__read_table.assert_has_calls([call('fuel_table'), call('fuel_table', suffix='_totals')])


@patch('anp_sales_miner.helpers.validator._read_table')
def test_validate_table_raising_mismatched_error(mock__read_table, fuel_table, fuel_table_totals):
    non_expected_year = fuel_table['year_month'].str.replace('2017', '2018')
    invalid_fuel_table = fuel_table.assign(year_month=non_expected_year)
    mock__read_table.side_effect = [invalid_fuel_table, fuel_table_totals]

    expected_message = 'There are mismatched records between processed and original totals!'
    with pytest.raises(AssertionError, match=expected_message):
        validate_table('fuel_table')


@patch('anp_sales_miner.helpers.validator._read_table')
def test_validate_table_raising_unequal_error(mock__read_table, fuel_table, fuel_table_totals):
    non_expected_volumes = np.where(
        fuel_table['year_month'] == '2016-04',
        fuel_table['volume'] + 0.1,
        fuel_table['volume']
    )
    invalid_fuel_table = fuel_table.assign(volume=non_expected_volumes)
    mock__read_table.side_effect = [invalid_fuel_table, fuel_table_totals]

    expected_message = 'There are unequal values for totals between processed and original totals!'
    with pytest.raises(AssertionError, match=expected_message):
        validate_table('fuel_table')


def test__read_table(mock_datalake_path, fuel_table):
    expect_df = fuel_table
    expect_df.to_parquet(f'{make_path(stage="clean", table="fuel_table")}')
    output_df = _read_table('fuel_table')

    assert output_df.equals(expect_df)


def test__read_table_with_suffix(mock_datalake_path, fuel_table_totals):
    expect_df = fuel_table_totals
    expect_df.to_parquet(f'{make_path(stage="clean", table="fuel_table_totals")}')
    output_df = _read_table('fuel_table', suffix='_totals')

    assert output_df.equals(expect_df)


def test__calculate_totals():
    input_df = pd.DataFrame([('2021-01', 10.0), ('2022-01', 5.0),
                             ('2022-01', 50.0), ('2022-02', 80.0)],
                            columns=['year_month', 'volume'])

    expect = pd.Series(data=[10.0, 55.0, 80.0],
                       index=['2021-01', '2022-01', '2022-02'],
                       name='volume')

    output = _calculate_totals(input_df)

    assert output.equals(expect)


@pytest.mark.parametrize(
    ('error_key', 'validation_error'),
    [('mismatched', 'There are mismatched records between processed and original totals!'),
     ('unequal', 'There are unequal values for totals between processed and original totals!')]
)
def test__get_error(error_key, validation_error):
    assert _get_error(error_key) == validation_error
