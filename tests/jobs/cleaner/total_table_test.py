from unittest.mock import patch

import pandas as pd

from anp_sales_miner.jobs.cleaner.total_table import TotalTable


@patch.object(TotalTable, 'read_parsed_table')
def test_run(mock_read_parsed_table, patch_datetime_now, monkeypatch, tmpdir):
    monkeypatch.setenv('DATALAKE_PATH', str(tmpdir))

    input_df = pd.read_csv('/var/anp_sales_miner/tests/fixtures/jobs/parsed_fuel_table_totals.csv')
    mock_read_parsed_table.return_value = input_df

    TotalTable('fuel_table').run()
    output_df = pd.read_parquet(f'{tmpdir}/clean/fuel_table_totals')

    expect_df = pd.read_csv('/var/anp_sales_miner/tests/fixtures/jobs/clean_fuel_table_totals.csv')

    assert sorted(output_df.columns) == sorted(expect_df.columns)

    output_df = output_df[expect_df.columns.tolist()]
    output_df = output_df.sort_values(output_df.columns.tolist()) \
                         .reset_index(drop=True) \
                         .astype('string')

    expect_df = expect_df.sort_values(expect_df.columns.tolist()) \
                         .reset_index(drop=True) \
                         .astype('string')

    assert output_df.equals(expect_df)
