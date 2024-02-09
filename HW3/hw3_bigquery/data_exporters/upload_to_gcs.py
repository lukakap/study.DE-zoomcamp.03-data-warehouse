from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

import pyarrow as pa
import pyarrow.parquet as pq

import os


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/angular-sign-demo-405419-aee0cafa32be.json"
project_id = 'angular-sign-demo-405419'
bucket_name = 'de-zoomcamp-hw-3'
object_key = 'green_taxi_trips.parquet'
table_name = 'green_taxi_trips'
root_path = f'{bucket_name}/{object_key}'

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:
    """
    Template for exporting data to a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    table = pa.Table.from_pandas(df)

    gcs = pa.fs.GcsFileSystem()

    pq.write_table(
        table,
        root_path,
        filesystem=gcs
    )
