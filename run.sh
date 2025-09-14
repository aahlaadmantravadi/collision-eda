#!/bin/bash
set -e

echo "🚀 Starting the end-to-end data pipeline setup..."

# Step 1: Start all services in the background
echo "--- Starting Docker services (Postgres, Metabase)..."
docker-compose up -d --build

# Step 2: Start Prefect and set up the block
echo "--- Starting Prefect Orion..."
prefect orion start &
sleep 10 # Wait for Orion to be ready
echo "--- Setting up Prefect SQLAlchemy block..."
python -c "from prefect_sqlalchemy import SqlAlchemyConnector; SqlAlchemyConnector(url='postgresql+psycopg2://root:root@pgdatabase:5432/MVC_db').save('psgres-connector', overwrite=True)"

# Step 3: Deploy the Prefect flow
echo "--- Deploying Prefect flow..."
prefect deploy --all

# Step 4: Run the data ingestion for all datasets
YEARS_JSON="[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]"
echo "--- Starting data ingestion for Crashes (C)..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=C" --param "years=$YEARS_JSON"

echo "--- Starting data ingestion for Vehicles (V)..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=V" --param "years=$YEARS_JSON"

echo "--- Starting data ingestion for Persons (P)..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=P" --param "years=$YEARS_JSON"

# Step 5: Run dbt models
echo "--- Running dbt models to transform data..."
cd dbt
dbt deps
dbt run
dbt run --select mvc_sum_all # Run final model again to ensure dependencies are met
cd ..

# Step 6: Final instructions for Metabase
echo "✅ Pipeline setup is complete!"
echo "-----------------------------------------------------"
echo "👉 Your final step is to set up Metabase:"
echo "1. Open Metabase: http://localhost:3001 (or find the forwarded port in your Codespace)"
echo "2. Create your admin account."
echo "3. Connect to the database with these settings:"
echo "   - Host: pgdatabase"
echo "   - Port: 5432"
echo "   - Database name: MVC_db"
echo "   - Database username: root"
echo "   - Database password: root"
echo ""
echo "After connecting, trigger dashboard creation with:"
echo "prefect deployment run 'MVC_main/MVC_flow' --param 'data_type=metabase' --param 'years=[\"YOUR_EMAIL\", \"YOUR_PASSWORD\"]'"
echo "-----------------------------------------------------"