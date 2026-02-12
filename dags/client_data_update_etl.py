"""
Client Data Update ETL DAG

Syncs pool-equipped listings from realestate DB to master DB.
Triggered after weekly_listings_etl completes.

Flow:
1. Extract new pool listings (last 2 days)
2. Extract removed pool listings (last 2 days)
3. Match against existing properties in master DB
4. Create property/pool objects for new unmatched listings
5. Mark removed listings as sold
6. Upsert properties, pools, listings to master DB
"""

import os
import tempfile
import pickle
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

# Import functions
from include.extract.client_data_extract import (
    extract_new_pool_listings,
    extract_removed_pool_listings,
)
from include.transform.client_data_transform import (
    match_existing_properties,
    transform_create_property_pool_objects,
    transform_mark_removed_listings,
)
from include.load.client_data_load import (
    upsert_properties,
    upsert_pools,
    upsert_listings,
    update_removed_listings,
)


# Default args
default_args = {
    'owner': 'james',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# DAG definition
with DAG(
    'client_data_update_etl',
    default_args=default_args,
    description='Sync pool listings from realestate DB to master DB',
    schedule=None,  # Triggered by weekly_listings_etl
    catchup=False,
    tags=['client-data', 'master-db', 'pools'],
) as dag:
    
    # Create working directory
    workdir = os.path.join(tempfile.gettempdir(), f"client_data_update_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
    os.makedirs(workdir, exist_ok=True)
    
    
    # Task 1: Extract new pool listings
    def extract_new_pools_task(**context):
        result = extract_new_pool_listings(workdir)
        context['ti'].xcom_push(key='new_pools_path', value=result['parquet_path'])
        context['ti'].xcom_push(key='new_pools_count', value=result['row_count'])
        return result
    
    extract_new = PythonOperator(
        task_id='extract_new_pool_listings',
        python_callable=extract_new_pools_task,
    )
    
    
    # Task 2: Extract removed pool listings
    def extract_removed_pools_task(**context):
        result = extract_removed_pool_listings(workdir)
        context['ti'].xcom_push(key='removed_pools_path', value=result['parquet_path'])
        context['ti'].xcom_push(key='removed_pools_count', value=result['row_count'])
        return result
    
    extract_removed = PythonOperator(
        task_id='extract_removed_pool_listings',
        python_callable=extract_removed_pools_task,
    )
    
    
    # Task 3: Match existing properties
    def match_properties_task(**context):
        ti = context['ti']
        new_path = ti.xcom_pull(task_ids='extract_new_pool_listings', key='new_pools_path')
        removed_path = ti.xcom_pull(task_ids='extract_removed_pool_listings', key='removed_pools_path')
        
        result = match_existing_properties(new_path, removed_path, workdir)
        
        ti.xcom_push(key='new_matched_path', value=result['new_matched_path'])
        ti.xcom_push(key='removed_matched_path', value=result['removed_matched_path'])
        return result
    
    match_properties = PythonOperator(
        task_id='match_existing_properties',
        python_callable=match_properties_task,
    )
    
    
    # Task 4: Transform - Create property/pool objects
    def transform_new_task(**context):
        ti = context['ti']
        new_matched_path = ti.xcom_pull(task_ids='match_existing_properties', key='new_matched_path')
        
        result = transform_create_property_pool_objects(new_matched_path, workdir)
        
        ti.xcom_push(key='properties_path', value=result['properties_path'])
        ti.xcom_push(key='pools_path', value=result['pools_path'])
        ti.xcom_push(key='listings_path', value=result['listings_path'])
        return result
    
    transform_new = PythonOperator(
        task_id='transform_create_property_pool',
        python_callable=transform_new_task,

    )
    
    
    # Task 5: Transform - Mark removed
    def transform_removed_task(**context):
        ti = context['ti']
        removed_matched_path = ti.xcom_pull(task_ids='match_existing_properties', key='removed_matched_path')
        
        result = transform_mark_removed_listings(removed_matched_path, workdir)
        
        ti.xcom_push(key='updates_path', value=result['updates_path'])
        return result
    
    transform_removed = PythonOperator(
        task_id='transform_mark_removed',
        python_callable=transform_removed_task,

    )
    
    
    # Task 6: Load properties
    def load_properties_task(**context):
        ti = context['ti']
        properties_path = ti.xcom_pull(task_ids='transform_create_property_pool', key='properties_path')
        
        result = upsert_properties(properties_path)
        
        # Store property_records as temporary file instead of XCOM due to size
        import pickle
        property_records_path = os.path.join(workdir, 'property_records.pkl')
        with open(property_records_path, 'wb') as f:
            pickle.dump(result['property_records'], f)
        
        ti.xcom_push(key='property_records_path', value=property_records_path)
        ti.xcom_push(key='property_count', value=len(result['property_records']))
        return result
    
    load_props = PythonOperator(
        task_id='load_properties',
        python_callable=load_properties_task,
    )
    
    
    # Task 7: Load pools
    def load_pools_task(**context):
        ti = context['ti']
        pools_new_path = ti.xcom_pull(task_ids='transform_create_property_pool', key='pools_new_path')
        pools_existing_path = ti.xcom_pull(task_ids='transform_create_property_pool', key='pools_existing_path')
        property_records_path = ti.xcom_pull(task_ids='load_properties', key='property_records_path')
        
        # Load property_records from temporary file
        import pickle
        with open(property_records_path, 'rb') as f:
            property_records = pickle.load(f)
        
        result = upsert_pools(pools_new_path, pools_existing_path, property_records)
        return result
    
    load_pool = PythonOperator(
        task_id='load_pools',
        python_callable=load_pools_task,

    )
    
    
    # Task 8: Load listings
    def load_listings_task(**context):
        ti = context['ti']
        listings_path = ti.xcom_pull(task_ids='transform_create_property_pool', key='listings_path')
        property_records_path = ti.xcom_pull(task_ids='load_properties', key='property_records_path')
        
        # Load property_records from temporary file
        import pickle
        with open(property_records_path, 'rb') as f:
            property_records = pickle.load(f)
        
        result = upsert_listings(listings_path, property_records)
        return result
    
    load_list = PythonOperator(
        task_id='load_listings',
        python_callable=load_listings_task,
    )
    
    
    # Task 9: Update removed listings
    def update_removed_task(**context):
        ti = context['ti']
        updates_path = ti.xcom_pull(task_ids='transform_mark_removed', key='updates_path')
        
        result = update_removed_listings(updates_path)
        return result
    
    update_removed = PythonOperator(
        task_id='update_removed_listings',
        python_callable=update_removed_task,
    )
    
    
    # Task dependencies
    [extract_new, extract_removed] >> match_properties
    match_properties >> [transform_new, transform_removed]
    transform_new >> load_props >> load_pool >> load_list
    transform_removed >> update_removed
