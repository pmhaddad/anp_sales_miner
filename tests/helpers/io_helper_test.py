import os

import pandas as pd
import pytest

from anp_sales_miner.helpers.io_helper import make_path, write_as_parquet


@pytest.fixture
def mock_datalake_path(monkeypatch, tmp_path):
    test_datalake = tmp_path / 'datalake'
    test_datalake.mkdir()
    monkeypatch.setenv('DATALAKE_PATH', str(test_datalake))


@pytest.fixture
def sample_dataframe():
    df = pd.DataFrame({'a': [0, 1, 2, 3],
                       'b': ['x', 'x', 'y', 'z']})
    df['b'] = df['b'].astype('category')
    return df


def test_make_path(mock_datalake_path):
    path = make_path(stage='a_dummy_stage', table='a_table_name')
    assert path == f'{os.getenv("DATALAKE_PATH")}/a_dummy_stage/a_table_name'


def test_make_path_with_parents_error(monkeypatch):
    monkeypatch.setenv('DATALAKE_PATH', 'with/parents/datalake')

    message = r"\[Errno 2\] No such file or directory: 'with/parents/datalake/a_dummy_stage'"
    with pytest.raises(FileNotFoundError, match=message):
        make_path(stage='a_dummy_stage', table='a_table_name')


def test_make_path_exists_ok_no_error(mock_datalake_path):
    path = make_path(stage='a_dummy_stage', table='a_table_name')
    path = make_path(stage='a_dummy_stage', table='a_table_name')
    assert path == f'{os.getenv("DATALAKE_PATH")}/a_dummy_stage/a_table_name'


def test_write_as_parquet(tmp_path, sample_dataframe):
    output_path = tmp_path / 'df'
    write_as_parquet(sample_dataframe, output_path=output_path, partitioning=['b'])
    assert sample_dataframe.equals(pd.read_parquet(output_path))


def test_write_as_parquet_overwrite_or_ignore(tmp_path, sample_dataframe):
    output_path = tmp_path / 'df'
    write_as_parquet(sample_dataframe, output_path=output_path, partitioning=['b'])
    assert sample_dataframe.equals(pd.read_parquet(output_path))

    changed_z_partition = pd.DataFrame({'a': [999], 'b': ['z']})
    write_as_parquet(changed_z_partition, output_path=output_path, partitioning=['b'])
    expected_df = sample_dataframe.copy()
    expected_df.loc[3, 'a'] = 999
    assert expected_df.equals(pd.read_parquet(output_path))
