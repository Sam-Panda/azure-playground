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

    # [START howto_operator_adf_run_pipeline]
    run_pipeline1: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline1",
        azure_data_factory_conn_id="adf-conn-1",
        pipeline_name="pipeline1", 
        parameters={"p_table_name": "Address", "p_schema_name": "SalesLT"},
        wait_for_termination=False
    )
    # [END howto_operator_adf_run_pipeline]

    # [START howto_operator_adf_run_pipeline_async]
    run_pipeline2: BaseOperator = AzureDataFactoryRunPipelineOperator(
        task_id="run_pipeline2",
        pipeline_name="pipeline2", 
        azure_data_factory_conn_id="adf-conn-2",
        parameters={"p_table_name": "Customer", "p_schema_name": "SalesLT"},
        wait_for_termination=False
    )

    pipeline_run_sensor: BaseOperator = AzureDataFactoryPipelineRunStatusSensor(
        task_id="pipeline_run_sensor",
        azure_data_factory_conn_id="adf-conn-2",
        run_id=cast(str, XComArg(run_pipeline2, key="run_id"))
        
    )
    # [END howto_operator_adf_run_pipeline_async]

    begin >> Label("No async wait") >> run_pipeline1
    begin >> Label("Do async wait with sensor") >> run_pipeline2
    [run_pipeline1, pipeline_run_sensor] >> end

    # Task dependency created via `XComArgs`:
    #   run_pipeline2 >> pipeline_run_sensor