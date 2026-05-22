# Prefect ETL Pipeline Demo

This project demonstrates a simple ETL (Extract, Transform, Load) pipeline using Prefect. The pipeline simulates processing customer purchase data through three main stages.

## Overview

The ETL pipeline consists of three main tasks:

1. **Extract** (`extract_data`): Simulates extracting customer data from a source system
2. **Transform** (`transform_data`): Processes and cleans the data, adding business logic
3. **Load** (`load_data`): Saves the processed data to multiple output formats

## Features

- **Data Generation**: Creates realistic sample customer data with purchase amounts, regions, and contact information
- **Data Transformation**: 
  - Rounds purchase amounts
  - Categorizes customers into tiers (Basic, Standard, Premium)
  - Adds discount eligibility logic
  - Cleans data by removing null values
- **Multiple Output Formats**: Saves data as CSV, JSON, and generates summary statistics
- **Prefect Integration**: Uses Prefect's `@task` and `@flow` decorators for orchestration

## Project Structure

```
prefect-practice/
├── Pipfile                 # Python dependencies
├── Pipfile.lock           # Locked dependency versions
├── etl_demo.py           # Main ETL pipeline code
├── README.md             # This file
└── output/               # Generated output files (created when running)
    ├── processed_customers.csv
    ├── processed_customers.json
    └── summary.json
```

## Installation

The project uses pipenv for dependency management. Dependencies are already configured:

- `prefect`: Workflow orchestration
- `pandas`: Data manipulation

## Running the Pipeline

### Run the Prefect Server

Docker makes this simple:

```
docker run -p 4200:4200 -d --rm prefecthq/prefect:3-latest -- prefect server start --host 0.0.0.0

# to open the web UI:
prefect dashboard open
```

### Option 1: Direct Execution
```bash
python etl_demo.py
```

### Option 2: Using Prefect CLI
```bash
# Run the flow
prefect deployment build etl_demo.py:etl_pipeline -n "etl-demo"
prefect deployment apply etl_pipeline-deployment.yaml
prefect deployment run "ETL Pipeline Demo/etl-demo"
```

## Output

The pipeline generates three output files in the `output/` directory:

1. **`processed_customers.csv`**: Clean, transformed customer data
2. **`processed_customers.json`**: Same data in JSON format
3. **`summary.json`**: Summary statistics including:
   - Total records processed
   - Total revenue
   - Average purchase amount
   - Number of premium customers
   - Regional distribution
   - Processing timestamp

## Sample Output

When you run the pipeline, you'll see output like:

```
🚀 Starting ETL Pipeline Demo
==================================================
🔄 Extracting data from source...
✅ Extracted 100 records
🔄 Transforming data...
✅ Transformed 100 records
📊 Customer tiers: {'Standard': 45, 'Basic': 35, 'Premium': 20}
🔄 Loading data to destination...
✅ Data loaded successfully!
📁 Files created:
   - output/processed_customers.csv
   - output/processed_customers.json
   - output/summary.json
💰 Total revenue: $45,234.56
👥 Premium customers: 20
==================================================
🎉 ETL Pipeline completed successfully!
```

## Customization

You can easily modify the pipeline by:

- Changing the data generation logic in `extract_data()`
- Adding new transformations in `transform_data()`
- Modifying output formats in `load_data()`
- Adding new tasks to the flow

## Next Steps

This demo can be extended with:
- Real data sources (databases, APIs, files)
- More complex transformations
- Error handling and retries
- Scheduling and monitoring
- Integration with cloud storage
- Data validation and quality checks 
