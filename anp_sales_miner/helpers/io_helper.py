import os
from pathlib import Path

import pyarrow
import pyarrow.dataset as dataset


def make_path(stage, table):
    datalake = os.getenv('DATALAKE_PATH')
    Path(os.sep.join([datalake, stage])).mkdir(parents=False, exist_ok=True)
    return os.sep.join([datalake, stage, table])


def write_as_parquet(df, output_path, partitioning):
    table = pyarrow.Table.from_pandas(df)

    dataset.write_dataset(
        table,
        base_dir=output_path,
        format='parquet',
        partitioning=partitioning,
        partitioning_flavor='hive',
        max_partitions=2048,
        existing_data_behavior='overwrite_or_ignore',
    )
