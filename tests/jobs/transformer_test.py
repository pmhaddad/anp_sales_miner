from unittest.mock import patch

import pandas as pd
from pyarrow.parquet import ParquetDataset

from anp_sales_miner.jobs.transformer import Transformer

FIXTURES_ROOT = '/var/anp_sales_miner/tests/fixtures'


@patch('anp_sales_miner.jobs.transformer.pd.read_parquet')
def test_run(mock_read_parquet, monkeypatch, tmpdir):
    monkeypatch.setenv('DATALAKE_PATH', str(tmpdir))

    input_df = pd.read_csv(f'{FIXTURES_ROOT}/jobs/clean_fuel_table.csv')
    mock_read_parquet.return_value = input_df
    Transformer('fuel_table').run()

    output_pq = ParquetDataset(f'{tmpdir}/transformed/anp_fuel_sales', use_legacy_dataset=False)

    assert output_pq.partitioning.schema.names == ['product', 'year_month']

    output_df = pd.read_parquet(f'{tmpdir}/transformed/anp_fuel_sales')
    expect_df = pd.read_csv(f'{FIXTURES_ROOT}/jobs/transformed_anp_fuel_sales.csv')

    assert output_df.equals(expect_df)
