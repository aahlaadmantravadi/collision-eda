#!/bin/bash
set -e

echo "🚀 Starting the end-to-end data pipeline setup..."

# Step 1: Forcefully clean up any old processes to guarantee a clean start
echo "--- Cleaning up previous server processes (if any)..."
if lsof -t -i:4200; then
    kill -9 $(lsof -t -i:4200)
    echo "Forcibly stopped lingering Prefect server."
fi
sleep 3

# Step 2: Start Docker services
echo "--- Starting Docker services (Postgres, Metabase)..."
docker compose up -d

# Step 3: Start Prefect Server & Agent in the correct, robust order
echo "--- Starting Prefect server..."
prefect server start --host 0.0.0.0 &
SERVER_PID=$!

echo "--- Waiting for Prefect server to be ready..."
# THIS IS THE DEFINITIVE FIX:
# We now use Prefect's own internal, robust health check command.
# This avoids all container networking issues with curl and localhost.
until prefect server health &> /dev/null; do
    printf '.'
    sleep 5
done
echo ""
echo "✅ Prefect server is ready."

# Now that the server is healthy, start the agent
echo "--- Starting Prefect agent to execute flows..."
prefect agent start -q default &
AGENT_PID=$!
sleep 5 # Allow agent time to register

echo "✅ Prefect agent is running."

# Step 4: Configure Prefect and deploy flows
echo "--- Setting up Prefect SQLAlchemy block and deploying flows..."
python -c "from prefect_sqlalchemy import SqlAlchemyConnector; SqlAlchemyConnector(url='postgresql+psycopg2://root:root@pgdatabase:5432/MVC_db').save('psgres-connector', overwrite=True)"
prefect deploy --all
echo "✅ Prefect configured and flows deployed."

# --- The rest of the script is now guaranteed to run ---

# Step 5: Run data ingestion for all datasets
YEARS_JSON="[2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023]"
echo "--- Starting data ingestion. This will take several minutes..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=C" --param "years=$YEARS_JSON"
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=V" --param "years=$YEARS_JSON"
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=P" --param "years=$YEARS_JSON"

echo "--- Waiting for data ingestion to complete..."
until [ $(prefect flow-run ls --limit 5 | grep -c "Completed") -ge 3 ]; do
    printf '.'
    sleep 15
done
echo ""
echo "✅ Data ingestion complete."

# Step 6: Run dbt models
echo "--- Running dbt models to transform data..."
cd dbt && dbt run && cd ..

# Step 7: Metabase setup
echo "✅ Data pipeline complete! Setting up visualization..."
echo "-----------------------------------------------------"
echo "1. Open Metabase: In the 'PORTS' tab, click the Globe icon next to port 3001."
echo "2. Create your admin account and return here."
read -p "Enter the email you used for Metabase admin: " METABASE_EMAIL
read -sp "Enter the password you used for Metabase admin: " METABASE_PASSWORD
echo ""

echo "--- Building your Metabase dashboard..."
prefect deployment run 'MVC_main/MVC_flow' --param "data_type=metabase" --param "years=[\"$METABASE_EMAIL\", \"$METABASE_PASSWORD\"]"

# Step 8: Send REAL Amplitude Tracking Event
echo "--- Sending user engagement event to Amplitude..."
prefect deployment run 'Amplitude_Event_Tracker/Amplitude_Event_Flow' --param "event_type=Dashboard_Created" --param "user_id=$METABASE_EMAIL"

# Step 9: Clean up background processes
echo "--- Shutting down Prefect server and agent..."
kill $AGENT_PID
kill $SERVER_PID

echo "🎉 All done! Your dashboard is ready in the 'NYC Collision Analysis' collection in Metabase."
echo "-----------------------------------------------------"