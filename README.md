# NYC Motor Vehicle Collision Analysis Pipeline

An end-to-end, containerized data pipeline that automatically ingests, processes, and visualizes NYC motor vehicle collision data from 2012 to 2023.

**Tech Stack:** Docker | PostgreSQL | Python | Prefect | dbt | Metabase

---

## How to Run (2 Steps)

This project is designed to run instantly in a pre-configured cloud environment using GitHub Codespaces.

### 1. Open in GitHub Codespaces

Click the button below to launch the project. This will build a complete, ready-to-run environment in your browser.

[![Open in GitHub Codespaces](https://github.com/codespaces/badge.svg)](https://codespaces.new/aahlaadmantravadi/collision-eda)

### 2. Run the Main Script

Once the Codespace is ready, a terminal will automatically open. Run the single setup script:

```bash
./run.sh
```

The script will handle everything automatically:

-> Start the PostgreSQL and Metabase services.

-> Download and process all collision datasets. (This will take some time).

-> Run dbt models to create analytical tables.

-> Prompt you for your Metabase login details to automatically build your dashboard.

That's it! Just follow the prompts in the terminal to complete the setup.
