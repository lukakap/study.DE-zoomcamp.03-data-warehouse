import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    year_to_download = 2022
    months_to_download = range(1, 13)

    parse_dates_green_taxi = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    df = None

    for month in months_to_download:
        month_str = str(month)
        if len(month_str) == 1:
            month_str = '0' + month_str

        url = f'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year_to_download}-{month_str}.parquet'
        
        df_part = pd.read_parquet(url)

        if df is None:
            df = df_part
        else:
            df = pd.concat([df, df_part], axis=0, ignore_index=True)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

    print(df.dtypes)
        
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
