import os
import pandas as pd
from time import time
from sqlalchemy import create_engine
from prefect import flow, task
from prefect_sqlalchemy import SqlAlchemyConnector
from metabase_api import Metabase_API
import pipeline_set

@task(log_prints=True, tags=["download"])
def download_data(data_type):
    """Downloads data based on the selected type or prepares for other actions."""
    if data_type in ["check", "metabase"]:
        return (data_type, data_type)

    if data_type.endswith(" reload"):
        data_type = data_type.split()[0]

    if data_type in ["C", "V", "P"]:
        url = getattr(pipeline_set, f"url_{data_type}")
        csv_name = f"MVC_{data_type}.csv"
        os.system(f"wget {url} -O {csv_name}")
        return (csv_name, data_type)
    else:
        return ("err", None)

@task(log_prints=True, tags=["setup"])
def setup_tables_and_engine(csv_name, years, data_type):
    """Creates database tables with the correct schema before loading data."""
    df = pd.read_csv(csv_name, nrows=1, low_memory=False)
    df = df[getattr(pipeline_set, f"sel_{data_type}")]
    df.rename(columns=(getattr(pipeline_set, f"sel_rename_{data_type}")), inplace=True)

    connection_block = SqlAlchemyConnector.load("psgres-connector")
    engine = connection_block.get_connection(begin=False)

    for i in years:
        df.head(n=0).to_sql(
            name=f"MVC_{data_type}_{i}",
            con=engine,
            dtype=(getattr(pipeline_set, f"sel_types_{data_type}")),
            if_exists='replace'
        )
    return engine

@task(log_prints=True, tags=["transform-load"])
def transform_and_load(years, csv_name, engine, data_type):
    """Transforms and loads data in chunks into the PostgreSQL database."""
    total_rows_loaded = 0
    start_time = time()
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000, low_memory=False)

    for df_chunk in df_iter:
        df = df_chunk[getattr(pipeline_set, f"sel_{data_type}")]
        df.rename(columns=(getattr(pipeline_set, f"sel_rename_{data_type}")), inplace=True)
        df.crash_date = pd.to_datetime(df.crash_date, errors='coerce').dt.date
        df.crash_time = pd.to_datetime(df.crash_time, format='%H:%M', errors='coerce').dt.time
        
        df.dropna(subset=['crash_date'], inplace=True)

        for year in years:
            df_temp = df.loc[pd.to_datetime(df.crash_date).dt.year == year]
            if not df_temp.empty:
                df_temp.to_sql(name=f"MVC_{data_type}_{year}", con=engine, if_exists='append', index=False)
                total_rows_loaded += len(df_temp)
        
        print(f"Loaded {total_rows_loaded} rows. Total time: {time() - start_time:.2f} seconds.")
    
    print("Finished ingesting data into the PostgreSQL database.")

@task(log_prints=True, tags=["check"])
def check_downloaded_data():
    """Checks and reports the row counts for each table in the database."""
    connection_block = SqlAlchemyConnector.load("psgres-connector")
    engine = connection_block.get_connection(begin=False)
    
    report = [['year', 'Crashes', 'Vehicles', 'Person']]
    for year in range(2012, 2024):
        row = [year]
        for data_type in ["C", "V", "P"]:
            try:
                count = pd.read_sql_query(f'SELECT COUNT(*) FROM "MVC_{data_type}_{year}"', con=engine).iloc[0, 0]
                row.append(int(count))
            except Exception:
                row.append(0)
        report.append(row)

    # Format and print the report
    header = f"{report[0][0]:>11}{report[0][1]:>11}{report[0][2]:>11}{report[0][3]:>11}"
    print("\n   Downloaded data report:\n")
    print(header)
    totals = [0, 0, 0]
    for row_data in report[1:]:
        print(f"{row_data[0]:>11}{f'{row_data[1]:,}':>11}{f'{row_data[2]:,}':>11}{f'{row_data[3]:,}':>11}")
        totals[0] += row_data[1]
        totals[1] += row_data[2]
        totals[2] += row_data[3]
    print(f"\n{'total':>11}{f'{totals[0]:,}':>11}{f'{totals[1]:,}':>11}{f'{totals[2]:,}':>11}")

@task(log_prints=True, tags=["metabase"])
def setup_metabase(credentials):
    """Connects to Metabase and automatically creates the dashboard and cards."""
    mb_login, mb_pass = credentials[0], credentials[1]
    
    try:
        mb = Metabase_API('http://metabase-app:3000', mb_login, mb_pass)
        print("Metabase connection successful.")
    except Exception as e:
        print(f"Metabase connection failed: {e}")
        return

    try:
        db_id = mb.get_item_id('database', "MVC_db")
        print("Database 'MVC_db' found.")
    except Exception:
        print("Database 'MVC_db' not found. Please connect it in Metabase first.")
        return

    try:
        collection_id = mb.get_item_id('collection', "MVC_collection")
        print("Collection 'MVC_collection' already exists.")
    except Exception:
        mb.create_collection("MVC_collection", parent_collection_name='Root')
        collection_id = mb.get_item_id('collection', "MVC_collection")
        print("Collection 'MVC_collection' created.")

    # Create or update cards
    for card_details in pipeline_set.CARD_DEFINITIONS:
        card_details['dataset_query']['database'] = db_id
        card_name = card_details['name']
        try:
            # Delete if exists to ensure it's up to date
            mb.delete_item('card', item_name=card_name, collection_name="MVC_collection")
            print(f"Deleted existing card: {card_name}")
        except Exception:
            pass # Card didn't exist, which is fine
        
        mb.create_card(custom_json=card_details, collection_id=collection_id)
        print(f"Card '{card_name}' created/updated.")

@flow(
    name="MVC_main",
    description="Main flow to download, process, and load NYC Motor Vehicle Collision data."
)
def MVC_main(data_type: str, years: list):
    """
    Main pipeline flow.
    - data_type: 'C', 'V', 'P', 'check', or 'metabase'.
    - years: List of years to process, or Metabase credentials.
    """
    csv_name, data_type_processed = download_data(data_type)

    if csv_name == "err":
        print("Error: Invalid data_type specified.")
    elif csv_name == "check":
        check_downloaded_data()
    elif csv_name == "metabase":
        setup_metabase(years)
    else:
        engine = setup_tables_and_engine(csv_name, years, data_type_processed)
        transform_and_load(years, csv_name, engine, data_type_processed)

if __name__ == '__main__':
    # Example run configuration
    DATA_TYPE = "C"  # Options: "C", "V", "P"
    YEARS_TO_PROCESS = [2022, 2023]
    MVC_main(data_type=DATA_TYPE, years=YEARS_TO_PROCESS)