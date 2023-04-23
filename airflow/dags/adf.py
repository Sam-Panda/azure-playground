from datetime import datetime, timedelta
from typing import cast
from airflow.models.xcom_arg import XComArg

from airflow.models import DAG, BaseOperator

try:
    from airflow.operators.empty import EmptyOperator
except ModuleNotFoundError:
    from airflow.operators.dummy import DummyOperator as EmptyOperator  # type: ignore

from airflow.providers.microsoft.azure.operators.data_factory import AzureDataFactoryRunPipelineOperator
from airflow.providers.microsoft.azure.sensors.data_factory import AzureDataFactoryPipelineRunStatusSensor
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

from airflow.operators.python_operator import PythonOperator

from airflow.utils.edgemodifier import Label


with DAG(
    dag_id="example_adf_run_pipeline",
    start_date=datetime(2022, 5, 14),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=3)       
    },
    default_view="graph",
) as dag:
    
    begin = EmptyOperator(task_id="begin")
    end = EmptyOperator(task_id="end")

   # [START operator_adf_run_pipeline_async]
    run_pipeline1: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        azure_data_factory_conn_id="adf-conn-1",
        pipeline_name="pipeline1", 
        parameters={"p_table_name": "Address", "p_schema_name": "SalesLT"},
        wait_for_termination=True
    )
   # [END operator_adf_run_pipeline_async]

    # [START operator_adf_run_pipeline_async]
    run_pipeline2: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline2",
        pipeline_name="pipeline2", 
        azure_data_factory_conn_id="adf-conn-2",
        parameters={"p_table_name": "Customer", "p_schema_name": "SalesLT"},
        wait_for_termination=True
    )
    # [END operator_adf_run_pipeline_async]

    # [START operator_databricks_job_pipeline_sync]

    default_args = {
        'source_path_table_1': 'adf1-eastus/SalesLT_Address.csv',
        'source_path_table_2': 'adf2-westus/saleslt_customer.csv',
        'Table1Name': 'saleslt_address',
        'Table2Name': 'saleslt_customer',
    }
    
    databricks_job_run = DatabricksRunNowOperator(
        task_id="databricks_job_run",
        default_args=default_args,
        databricks_conn_id="databricks-conn-1",
        job_id=896625907610388,
        wait_for_termination=True

    )


    # [END operator_databricks_job_pipeline_sync]

   
   
    # Task dependency created via `>>`:

    begin >> Label("Trigger the First ADF pipeline") >> run_pipeline1
    begin >> Label("Trigger the second ADF pipeline") >> run_pipeline2
    [run_pipeline1, run_pipeline2] >> databricks_job_run
    databricks_job_run >> end
  

    # Task dependency created via `XComArgs`:
    #   run_pipeline2 >> pipeline_run_sensor