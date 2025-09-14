# NYC Motor Vehicle Collision Analysis Pipeline

This project provides an end-to-end data pipeline for analyzing NYC Motor Vehicle Collision data. It automates data ingestion, transformation, and visualization using Docker, Prefect, dbt, and Metabase.

## Easiest Way to Run: GitHub Codespaces

You can run this entire project in your browser with a single click, without installing anything on your computer.

1.  **Open in Codespace**: Click the "Code" button on this repository page, go to the "Codespaces" tab, and click "Create codespace on main". This will set up a complete, ready-to-run environment for you.

2.  **Run the Setup Script**: Once the Codespace is ready, a terminal will open. Run the main script:
    ```bash
    ./run.sh
    ```
This script handles everything: starting services, ingesting and processing all the data (this will take some time), running the dbt models, and preparing Metabase.

## The Final Step: View Your Dashboard

The `run.sh` script will provide you with a URL for Metabase.

-   Open the URL and create your admin account.
-   When prompted to add a database, use the following settings:
    -   **Database type**: `PostgreSQL`
    -   **Display name**: `MVC_db`
    -   **Host**: `pgdatabase`
    -   **Port**: `5432`
    -   **Database name**: `MVC_db`
    -   **Database username**: `root`
    -   **Database password**: `root`
-   Click **Save**. Your dashboard will be automatically created and visible in the **"MVC_collection"**.

## Alternative: Running Locally

If you prefer to run this on your own machine:
1.  Ensure you have **Docker** and **Docker Compose** installed and running.
2.  Clone the repository.
3.  Open a terminal (like Git Bash or WSL) and run the setup script:
    ```bash
    ./run.sh
    ```
4.  Follow the same final step above to connect Metabase.