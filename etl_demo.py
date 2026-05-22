from prefect import flow, task
import pandas as pd
import random
from datetime import datetime
import json
import os

@task(name="extract_data")
def extract_data():
    """Simulate extracting data from a source system"""
    print("🔄 Extracting data from source...")
    
    # Simulate some sample data
    data = {
        'customer_id': range(1, 101),
        'name': [f"Customer_{i}" for i in range(1, 101)],
        'email': [f"customer_{i}@example.com" for i in range(1, 101)],
        'purchase_amount': [random.uniform(10.0, 1000.0) for _ in range(100)],
        'purchase_date': [datetime.now().strftime('%Y-%m-%d') for _ in range(100)],
        'region': [random.choice(['North', 'South', 'East', 'West']) for _ in range(100)]
    }
    
    df = pd.DataFrame(data)
    print(f"✅ Extracted {len(df)} records")
    return df

@task(name="transform_data")
def transform_data(df: pd.DataFrame):
    """Transform and clean the extracted data"""
    print("🔄 Transforming data...")
    
    # Create a copy to avoid modifying the original
    transformed_df = df.copy()
    
    # Add some transformations
    transformed_df['purchase_amount_rounded'] = transformed_df['purchase_amount'].round(2)
    transformed_df['customer_tier'] = transformed_df['purchase_amount'].apply(
        lambda x: 'Premium' if x > 500 else 'Standard' if x > 100 else 'Basic'
    )
    transformed_df['extraction_timestamp'] = datetime.now().isoformat()
    
    # Clean data (remove any null values)
    transformed_df = transformed_df.dropna()
    
    # Add some business logic
    transformed_df['discount_eligible'] = transformed_df['purchase_amount'] > 200
    
    print(f"✅ Transformed {len(transformed_df)} records")
    print(f"📊 Customer tiers: {transformed_df['customer_tier'].value_counts().to_dict()}")
    
    return transformed_df

@task(name="load_data")
def load_data(df: pd.DataFrame):
    """Load the transformed data to destination"""
    print("🔄 Loading data to destination...")
    
    # Create output directory if it doesn't exist
    os.makedirs('output', exist_ok=True)
    
    # Save to CSV
    csv_path = 'output/processed_customers.csv'
    df.to_csv(csv_path, index=False)
    
    # Save to JSON for demonstration
    json_path = 'output/processed_customers.json'
    df.to_json(json_path, orient='records', indent=2)
    
    # Generate summary statistics
    summary = {
        'total_records': len(df),
        'total_revenue': df['purchase_amount'].sum(),
        'average_purchase': df['purchase_amount'].mean(),
        'premium_customers': len(df[df['customer_tier'] == 'Premium']),
        'discount_eligible_customers': len(df[df['discount_eligible'] == True]),
        'region_distribution': df['region'].value_counts().to_dict(),
        'processed_at': datetime.now().isoformat()
    }
    
    # Save summary
    summary_path = 'output/summary.json'
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"✅ Data loaded successfully!")
    print(f"📁 Files created:")
    print(f"   - {csv_path}")
    print(f"   - {json_path}")
    print(f"   - {summary_path}")
    print(f"💰 Total revenue: ${summary['total_revenue']:,.2f}")
    print(f"👥 Premium customers: {summary['premium_customers']}")
    
    return summary

@flow(name="ETL Pipeline Demo")
def etl_pipeline():
    """Main ETL pipeline flow"""
    print("🚀 Starting ETL Pipeline Demo")
    print("=" * 50)
    
    # Extract data
    raw_data = extract_data()
    
    # Transform data
    processed_data = transform_data(raw_data)
    
    # Load data
    summary = load_data(processed_data)
    
    print("=" * 50)
    print("🎉 ETL Pipeline completed successfully!")
    
    return summary

if __name__ == "__main__":
    # Run the flow
    result = etl_pipeline() 