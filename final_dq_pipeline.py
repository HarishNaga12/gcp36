
from google.cloud import bigquery
import pendulum
import logging
import argparse

from DQ_functions import execution_status, send_email
from DQ.primary_engine import pe
from DQ.DE_health import de_health
from DQ.Table_Health import tl_health

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# DAG default arguments
default_args = {
    'owner': 'RACF: MOUBAAN',
    'depends_on_past': False,
    'retries': 0,
}

@dag(
    dag_id='DQ-Parallel-Processing',
    schedule='10 * * * 1-6',
    start_date=pendulum.datetime(2025, 4, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["DQ", "MRIA"],
)
def init_dq_pipeline():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task(task_id='process_table_rules')
    def process_table_rules():
        project_id = "taw-centralrpt-prod-5796"
        client = bigquery.Client(project=project_id)

        # Get tables refreshed today but not processed
        sql = """
        SELECT r.*, e.table_processed_today
        FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde.STG_SRC_TABLE_REFRESH_METADATA` r
        JOIN `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_RULE_EXECUTIONS` e
        ON r.DATASET_ID = e.data_schema AND r.TABLE_NAME = e.data_table
        WHERE r.REFRESHED_TODAY_FLAG = 'Y' AND e.table_processed_today = 'N'
        """
        df = client.query(sql).result().to_dataframe()

        if df.shape[0] == 0:
            print("No rules to process at this time!")
            return

        tables = list(df['TABLE_NAME'].values)

        # Process Rules
        print("Processing rules")
        rules_query = "SELECT * FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`"
        df_rules = client.query(rules_query).result().to_dataframe()
        pe(df_rules, tables)
        print("...")

        # Process Data Element Health
        print("Processing Data Element Health")
        de_health(tables)
        print("...")

        # Process Table Health
        print("Processing Table Health")
        tl_health(tables)
        print("...")

        # Update processed tables
        tables_where_clause = ','.join([f"'{t}'" for t in tables])
        table_processed_query = f"""
        UPDATE `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_RULE_EXECUTIONS`
        SET table_processed_today = 'Y'
        WHERE data_table IN ({tables_where_clause})
        """
        update_processed_table_query_job = client.query(table_processed_query)
        update_processed_table_query_job.result()

        # Send Email
        try:
            table_result = execution_status(tables)
            email_message = f"{table_result}"
            to_email = ['anthony_moubarak@keybank.com', 'Anil_Kumar@keybank.com']
            send_email(to_email, email_message)
        except Exception as e:
            print("Error sending email:", e)

    start >> process_table_rules() >> end

init_dq_pipeline()
