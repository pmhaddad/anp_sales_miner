from unittest.mock import DEFAULT, patch

import pandas as pd
import pytest

from anp_sales_miner.jobs.cleaner import Cleaner


@pytest.fixture(scope='module')
def cleaner():
    class TestCleaner(Cleaner):
        def melt_table(self, df):
            return df

    return TestCleaner('test_table_name')


def test_cleaner___init__(cleaner):
    assert cleaner.table_name == 'test_table_name'
    assert cleaner.rename_map == {'COMBUSTÍVEL': 'product', 'ANO': 'year', 'REGIÃO': 'region',
                                  'ESTADO': 'uf', 'UNIDADE': 'unit'}
    assert cleaner.month_map == {'Jan': '01', 'Janeiro': '01', 'Fev': '02', 'Fevereiro': '02',
                                 'Mar': '03', 'Março': '03', 'Abr': '04', 'Abril': '04',
                                 'Mai': '05', 'Maio': '05', 'Jun': '06', 'Junho': '06',
                                 'Jul': '07', 'Julho': '07', 'Ago': '08', 'Agosto': '08',
                                 'Set': '09', 'Setembro': '09', 'Out': '10', 'Outubro': '10',
                                 'Nov': '11', 'Novembro': '11', 'Dez': '12', 'Dezembro': '12'}
    assert cleaner.table_schema == {'year_month': 'string', 'uf': 'string', 'product': 'string',
                                    'unit': 'string', 'volume': 'double',
                                    'created_at': 'datetime64[ms]'}


def test_cleaner___init__directly():
    expected_message = "Can't instantiate abstract class Cleaner with abstract methods melt_table"
    with pytest.raises(TypeError, match=expected_message):
        Cleaner('test_table_name')


@patch('anp_sales_miner.jobs.cleaner.pd', autospec=True)
def test_read_parsed_table(mock_pandas, cleaner):
    cleaner.read_parsed_table()
    mock_pandas.read_csv.assert_called_once_with(
        '/var/anp_sales_miner/datalake/parsed/test_table_name.csv', engine='python'
    )


def test_rename_columns(cleaner):
    input_df = pd.DataFrame([('Gás', 2022, 'Norte', 'AM', 'm3')],
                            columns=['COMBUSTÍVEL', 'ANO', 'REGIÃO', 'ESTADO', 'UNIDADE'])

    expect_df = pd.DataFrame([('Gás', 2022, 'Norte', 'AM', 'm3')],
                             columns=['product', 'year', 'region', 'uf', 'unit'])

    output_df = cleaner.rename_columns(input_df)
    assert output_df.equals(expect_df)


def test_melt_table(cleaner):
    class TestCleaner(Cleaner):
        def melt_table(self):
            super().melt_table()

    expected_message = 'This method must be overwritten by a child class!'
    with pytest.raises(NotImplementedError, match=expected_message):
        TestCleaner('test_table_name').melt_table()


def test_clean_column_syntax(cleaner):
    input_df = pd.DataFrame([(2022, 'Jan'), (2022.0, 'Janeiro'), (2020.0, 'Abr'), (2020, 'Mar')],
                            columns=['year', 'month'])

    expect_df = pd.DataFrame([('2022', '01'), ('2022', '01'), ('2020', '04'), ('2020', '03')],
                             columns=['year', 'month'])

    output_df = cleaner.clean_columns_syntax(input_df)
    assert output_df.equals(expect_df)


def test_create_columns(cleaner, patch_datetime_now, test_time):
    input_df = pd.DataFrame([('2001', '10'), ('2002', '11')],
                            columns=['year', 'month'])

    expect_df = pd.DataFrame([('2001', '10', '2001-10', test_time),
                              ('2002', '11', '2002-11', test_time)],
                             columns=['year', 'month', 'year_month', 'created_at'])

    output_df = cleaner.create_columns(input_df)
    assert output_df.equals(expect_df)


def test_set_schema(cleaner, test_time):
    input_df = pd.DataFrame(
        [('Gás', '2001', 'Norte', 'AM', 'm3', '1000.0', '2001-10', test_time),
         ('Gás', '2001', 'Norte', 'AM', 'm3', '2000.0', '2001-11', test_time)],
        columns=['product', 'year', 'region', 'uf', 'unit', 'volume', 'year_month', 'created_at']
    )

    expect_df = pd.DataFrame(
        [('2001-10', 'AM', 'Gás', 'm3', 1000.0, test_time),
         ('2001-11', 'AM', 'Gás', 'm3', 2000.0, test_time)],
        columns=['year_month', 'uf', 'product', 'unit', 'volume', 'created_at']
    )
    convert_obj_to_str = ['year_month', 'uf', 'product', 'unit']
    expect_df[convert_obj_to_str] = expect_df[convert_obj_to_str].astype('string')

    output_df = cleaner.set_schema(input_df)
    assert output_df.equals(expect_df)


@patch('anp_sales_miner.jobs.cleaner.write_as_parquet', autospec=True)
@patch.multiple(Cleaner, read_parsed_table=DEFAULT, rename_columns=DEFAULT, melt_table=DEFAULT,
                clean_columns_syntax=DEFAULT, create_columns=DEFAULT, set_schema=DEFAULT,
                autospec=True)
def test_run(mock_write_parquet, cleaner, monkeypatch, tmpdir, **cleaner_mocks):
    monkeypatch.setenv('DATALAKE_PATH', str(tmpdir))

    mock_df = pd.DataFrame()

    for mock in cleaner_mocks.values():
        mock.return_value = mock_df

    cleaner.run()

    cleaner_mocks['read_parsed_table'].assert_called_once()
    cleaner_mocks['rename_columns'].assert_called_once_with(cleaner, mock_df)
    cleaner_mocks['melt_table'].assert_not_called()  # TestCleaner's melt_table is being called
    cleaner_mocks['clean_columns_syntax'].assert_called_once_with(cleaner, mock_df)
    cleaner_mocks['create_columns'].assert_called_once_with(cleaner, mock_df)
    cleaner_mocks['set_schema'].assert_called_once_with(cleaner, mock_df)
    mock_write_parquet.assert_called_once_with(
        mock_df, output_path=f'{tmpdir}/clean/test_table_name',
        partitioning=['product', 'year_month']
    )
