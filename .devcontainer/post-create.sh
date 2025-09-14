#!/bin/bash
echo "Codespace created. Setting up project..."
pip install -r requirements.txt
(cd dbt && dbt deps)
chmod +x run.sh
echo "Setup complete. You can now run the project with ./run.sh"