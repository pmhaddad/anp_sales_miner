from unittest.mock import patch

import pandas as pd

from anp_sales_miner.jobs.cleaner.fuel_table import FuelTable


@patch.object(FuelTable, 'read_parsed_table')
def test_run(mock_read_parsed_table, patch_datetime_now, monkeypatch, tmpdir):
    monkeypatch.setenv('DATALAKE_PATH', str(tmpdir))

    input_df = pd.read_csv('/var/anp_sales_miner/tests/fixtures/jobs/parsed_fuel_table.csv')
    mock_read_parsed_table.return_value = input_df

    FuelTable('fuel_table').run()
    output_df = pd.read_parquet(f'{tmpdir}/clean/fuel_table')

    expect_df = pd.read_csv('/var/anp_sales_miner/tests/fixtures/jobs/clean_fuel_table.csv')

    assert sorted(output_df.columns) == sorted(expect_df.columns)

    output_df = output_df[expect_df.columns.tolist()]
    output_df = output_df.sort_values(output_df.columns.tolist()) \
                         .reset_index(drop=True) \
                         .astype('string')

    expect_df = expect_df.sort_values(expect_df.columns.tolist()) \
                         .reset_index(drop=True) \
                         .astype('string')

    assert output_df.equals(expect_df)
