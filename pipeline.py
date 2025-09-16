import os
import pandas as pd
from time import time
import requests
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
            if_exists='replace',
            index=False
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
        
        # Convert to datetime once for efficiency
        df['crash_date_dt'] = pd.to_datetime(df.crash_date, errors='coerce')
        df.crash_time = pd.to_datetime(df.crash_time, format='%H:%M', errors='coerce').dt.time
        df.dropna(subset=['crash_date_dt'], inplace=True)

        for year in years:
            df_temp = df[df.crash_date_dt.dt.year == year]
            if not df_temp.empty:
                # Drop the temporary datetime column before loading
                df_to_load = df_temp.drop(columns=['crash_date_dt'])
                df_to_load.to_sql(name=f"MVC_{data_type}_{year}", con=engine, if_exists='append', index=False)
                total_rows_loaded += len(df_to_load)
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
    header = f"{report[0][0]:>11}{report[0][1]:>11}{report[0][2]:>11}{report[0][3]:>11}"
    print("\n   Downloaded data report:\n")
    print(header)
    totals = [0, 0, 0]
    for row_data in report[1:]:
        print(f"{row_data[0]:>11}{f'{row_data[1]:,}':>11}{f'{row_data[2]:,}':>11}{f'{row_data[3]:,}':>11}")
        totals[0] += row_data[1]; totals[1] += row_data[2]; totals[2] += row_data[3]
    print(f"\n{'total':>11}{f'{totals[0]:,}':>11}{f'{totals[1]:,}':>11}{f'{totals[2]:,}':>11}")

@task(log_prints=True, tags=["metabase"])
def setup_metabase(credentials):
    """Connects to Metabase and automatically creates the dashboard and cards."""
    mb_login, mb_pass = credentials[0], credentials[1]
    try:
        mb = Metabase_API('http://metabase-app:3000/', mb_login, mb_pass)
        print("Metabase connection successful.")
    except Exception as e:
        print(f"Metabase connection failed: {e}"); return
    try:
        db_id = mb.get_item_id('database', "MVC_db")
    except Exception:
        print("Database 'MVC_db' not found. Please connect it in Metabase first."); return
    try:
        collection_id = mb.get_item_id('collection', "NYC Collision Analysis")
    except Exception:
        mb.create_collection("NYC Collision Analysis", parent_collection_name='Root'); collection_id = mb.get_item_id('collection', "NYC Collision Analysis")
    for card_details in pipeline_set.CARD_DEFINITIONS:
        card_details['dataset_query']['database'] = db_id; card_name = card_details['name']
        try:
            mb.delete_item('card', item_name=card_name, collection_name="NYC Collision Analysis")
        except Exception:
            pass
        mb.create_card(custom_json=card_details, collection_id=collection_id)
        print(f"Card '{card_name}' created/updated.")

@task(log_prints=True, tags=["analytics-event"])
def send_amplitude_event(event_type: str, user_id: str):
    """Sends a real tracking event to Amplitude's HTTP API."""
    api_key = os.getenv("AMPLITUDE_API_KEY")
    if not api_key:
        print("ERROR: AMPLITUDE_API_KEY secret is not set. Skipping event tracking.")
        return
    
    print(f"\n--- Sending Real Amplitude Event ---"); print(f"Event Type: {event_type}"); print(f"User ID: {user_id}")
    
    url = "https://api2.amplitude.com/2/httpapi"
    payload = {
        "api_key": api_key,
        "events": [{"user_id": user_id, "event_type": event_type, "event_properties": {"project_name": "NYC Collision Analysis"}}]
    }
    
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        print("SUCCESS: Event sent to Amplitude and received a 200 OK response.")
    except Exception as e:
        print(f"ERROR: Failed to send event to Amplitude: {e}")
    print("--- Amplitude Event Tracking Complete ---\n")

@flow(name="Amplitude_Event_Tracker")
def amplitude_event_flow(event_type: str, user_id: str):
    send_amplitude_event(event_type, user_id)

@flow(name="MVC_main")
def MVC_main(data_type: str, years: list):
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