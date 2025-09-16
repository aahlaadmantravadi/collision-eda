#!/bin/bash
set -e

echo "🚀 Starting the end-to-end data pipeline setup..."

# Step 1: Start all services in the background
echo "--- Starting Docker services (Postgres, Metabase)..."
docker compose up -d

# Step 2: Start Prefect and set up the block
echo "--- Starting Prefect Orion..."
prefect orion start &
sleep 15
echo "--- Setting up Prefect SQLAlchemy block for Postgres..."
python -c "from prefect_sqlalchemy import SqlAlchemyConnector; SqlAlchemyConnector(database='MVC_db', driver='postgresql+psycopg2', username='root', password='root', host='pgdatabase', port=5432).save('psgres-connector', overwrite=True)"

# Step 3: Deploy all Prefect flows
echo "--- Deploying Prefect flows..."
prefect deploy --all

# Step 4: Run data ingestion for all datasets
YEARS_JSON="[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]"
echo "--- Starting data ingestion. This will take several minutes..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=C" --param "years=$YEARS_JSON"
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=V" --param "years=$YEARS_JSON"
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=P" --param "years=$YEARS_JSON"

# Step 5: Run dbt models, including the new geospatial model
echo "--- Running dbt models to transform data..."
cd dbt && dbt run && cd ..

# Step 6: Metabase setup
echo "✅ Data pipeline complete! Setting up visualization..."
echo "-----------------------------------------------------"
echo "1. Open Metabase: In the 'PORTS' tab, click the Globe icon next to port 3001."
echo "2. Create your admin account and return here."
read -p "Enter the email you used for Metabase admin: " METABASE_EMAIL
read -sp "Enter the password you used for Metabase admin: " METABASE_PASSWORD
echo ""

echo "--- Building your Metabase dashboard..."
sleep 5
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=metabase" --param "years=[\"$METABASE_EMAIL\", \"$METABASE_PASSWORD\"]"

# Step 7: Send REAL Amplitude Tracking Event
echo "--- Sending user engagement event to Amplitude..."
prefect deployment run 'Amplitude_Event_Tracker/Amplitude_Event_Flow' --param "event_type=Dashboard_Created" --param "user_id=$METABASE_EMAIL"

echo "🎉 All done! Your dashboard is ready in the 'NYC Collision Analysis' collection in Metabase."
echo "-----------------------------------------------------"```