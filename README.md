# Instructions
**Goal:** Pre-process Product Label indication information for use.

1. Make sure that Research DB configuration is set-up in your local environment.
```
export AWS_DEV_POSTGRES_DB_HOST="research-postgresql.ch0zjyulprxp.us-east-1.rds.amazonaws.com"
export AWS_DEV_POSTGRES_DB_USER="<YOUR_USERNAME>"
export AWS_DEV_POSTGRES_DB_PASSWORD="<YOUR_PASSWORD>"
export AWS_DEV_POSTGRES_DB_PORT="5432"
```

2. Run the script - this will output a series of `output_<number>.csv` CSV files in the root directory.
```
python openfda_data/product_label_indication_extraction.py
```

3. Import the CSV files into this Google Sheets: [Link](https://docs.google.com/spreadsheets/d/1uAeymmrmDZcnG2tCl8vXUib37ECBCOH-mfCFFM4rsVg/edit?gid=671415297#gid=671415297)
    - Make sure that you append the data in ascending order as only the first output CSV contain headers.
    - When appending, make sure to select "Append to current sheet".

4. Manually kick off the Rivery workflow: [Link to Rivery job](https://console.rivery.io/river/620eb59b19694a051c1a58ee/620eb62719694a051d57bbaa/river/671a64cb70c335f1d3950bb1)
