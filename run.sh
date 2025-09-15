#!/bin/bash
set -e

echo "🚀 Starting the end-to-end data pipeline setup..."

# Step 1: Start all services in the background
echo "--- Starting Docker services (Postgres, Metabase)..."
docker compose up -d

# Step 2: Start Prefect and set up the block
echo "--- Starting Prefect Orion..."
prefect orion start &
sleep 15 # Wait a bit longer for Orion to be fully ready
echo "--- Setting up Prefect SQLAlchemy block for Postgres..."
# THIS IS THE CORRECTED LINE:
python -c "from prefect_sqlalchemy import SqlAlchemyConnector; SqlAlchemyConnector(database='MVC_db', driver='postgresql+psycopg2', username='root', password='root', host='pgdatabase', port=5432).save('psgres-connector', overwrite=True)"

# Step 3: Deploy the Prefect flow
echo "--- Deploying Prefect flow..."
prefect deploy --all

# Step 4: Run the data ingestion for all datasets
YEARS_JSON="[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]"
echo "--- Starting data ingestion for Crashes (C). This may take several minutes..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=C" --param "years=$YEARS_JSON"

echo "--- Starting data ingestion for Vehicles (V)..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=V" --param "years=$YEARS_JSON"

echo "--- Starting data ingestion for Persons (P)..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=P" --param "years=$YEARS_JSON"

# Step 5: Run dbt models
echo "--- Running dbt models to transform data in the warehouse..."
cd dbt
dbt deps
dbt run
dbt run --select mvc_sum_all # Run final model again to ensure dependencies are met
cd ..

# Step 6: Final instructions for Metabase
echo "✅ Data pipeline is complete!"
echo "-----------------------------------------------------"
echo "👉 Your final step is to set up Metabase:"
echo "1. Open Metabase: In the 'PORTS' tab below, click the Globe icon next to port 3001."
echo "2. In the browser tab that opens, create your admin account."
echo "3. Come back to this terminal and follow the prompts."
echo ""

# Prompt user for their Metabase credentials
read -p "Enter the email you used for Metabase admin: " METABASE_EMAIL
read -sp "Enter the password you used for Metabase admin: " METABASE_PASSWORD
echo ""

echo "--- Connecting Metabase to the database and building your dashboard..."
sleep 10 # Give Metabase a moment to be fully ready

# Run the Prefect flow to set up Metabase
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=metabase" --param "years=[\"$METABASE_EMAIL\", \"$METABASE_PASSWORD\"]"

echo "🎉 All done! Your dashboard is ready in the 'MVC_collection' in Metabase."
echo "-----------------------------------------------------"