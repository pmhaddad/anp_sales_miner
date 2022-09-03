from unittest.mock import call, patch

import pandas as pd
import pytest

from anp_sales_miner.helpers.profiler import profile_table


@pytest.fixture
def test_df():
    return pd.DataFrame([('2001-01', ), ('2001-02', ), ('2001-03', )],
                        columns=['year_month'])


@patch('anp_sales_miner.helpers.profiler.ProfileReport', autospec=True)
@patch('anp_sales_miner.helpers.profiler.pd', autospec=True)
def test_profile_table(mock_pandas, mock_profile_report, test_df, monkeypatch, tmpdir):
    monkeypatch.setenv('DATALAKE_PATH', str(tmpdir))
    mock_pandas.read_parquet.return_value = test_df

    profile_table()

    mock_pandas.read_parquet.assert_called_once_with(f'{tmpdir}/transformed/anp_fuel_sales')
    mock_profile_report.assert_called_once_with(test_df, title='ANP Fuel Sales Profiling Report')
    assert mock_profile_report.method_calls == [call().to_file('reports/anp_fuel_sales.html')]
