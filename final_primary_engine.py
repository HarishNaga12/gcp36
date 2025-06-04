
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import numpy as np
from pandas.tseries.offsets import MonthEnd
from dataclasses import dataclass
import uuid
import time
from decimal import Decimal
# Local implementation of rule checks
from final_functions import standard_check, variance_check, ptp_check

client = bigquery.Client()
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

rules_results_table = 'taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_RESULTS'
execution_status_table = 'taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION_STATUS'

def process_rule(row):
    try:
        snapshot_date = date.today().strftime("%Y-%m-%d")
        execution_id = str(uuid.uuid4()).split("-")[0]
        rule_behavior = row['RULE_BEHAVIOR']
        data_column = row['DATA_COLUMN']
        cde_flag = row['CDE_FLAG']
        rule_id = row['RULE_ID']
        threshold_type = row['THRESHOLD_TYPE']
        amber_th = int(row['AMBER_THRESHOLD'])
        red_th = int(row['RED_THRESHOLD'])
        rule_level = 'Table' if data_column is None else 'Data Element'
        execution_start_time = time.time()
        execution_start_timestamp = datetime.fromtimestamp(execution_start_time)

        if rule_behavior == 'Standard':
            result, numerator_result, denominator_result, rag, err_msg = standard_check(rule_id)
        elif rule_behavior == 'Variance':
            result, numerator_result, denominator_result, rag, err_msg = variance_check(rule_id)
        else:
            result, numerator_result, denominator_result, rag, err_msg = ptp_check(rule_id)

        if numerator_result is None or (denominator_result is None and threshold_type.lower() != 'count'):
            print("Error for", rule_id, err_msg)
            return None, {
                'execution_id': execution_id,
                'rule_id': rule_id,
                'snapshot_date': snapshot_date,
                'is_latest_snapshot_flag': 'Y',
                'is_successful': 'N',
                'error_message': err_msg
            }

        execution_end_time = time.time()
        execution_end_timestamp = datetime.fromtimestamp(execution_end_time)

        results = {
            'rule_id': rule_id,
            'rule_level': rule_level,
            'execution_id': execution_id,
            'snapshot_date': snapshot_date,
            'cde_flag': cde_flag,
            'is_latest_snapshot_flag': 'Y',
            'execution_start': execution_start_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
            'execution_end': execution_end_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f UTC"),
            'execution_duration_seconds': (execution_end_time - execution_start_time),
            'numerator_result': numerator_result,
            'denominator_result': denominator_result,
            'rule_result': result,
            'threshold_type': threshold_type,
            'amber_threshold': amber_th,
            'red_threshold': red_th,
            'result_rag': rag
        }

        return results, {
            'execution_id': execution_id,
            'rule_id': rule_id,
            'snapshot_date': snapshot_date,
            'is_latest_snapshot_flag': 'Y',
            'is_successful': 'Y',
            'error_message': None
        }

    except Exception as e:
        print(f"Error for rule id: {rule_id}", e)
        return None, {
            'execution_id': execution_id,
            'rule_id': rule_id,
            'snapshot_date': snapshot_date,
            'is_latest_snapshot_flag': 'Y',
            'is_successful': 'N',
            'error_message': str(e)
        }

def pe(df, tables):
    trim_query = '''
    UPDATE `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`
    SET RULE_ID = TRIM(RULE_ID),
        STATUS = TRIM(STATUS),
        DATA_SOURCE = TRIM(DATA_SOURCE),
        DATA_SCHEMA = TRIM(DATA_SCHEMA),
        DATA_TABLE = TRIM(DATA_TABLE),
        DATA_COLUMN = TRIM(DATA_COLUMN),
        CDE_FLAG = TRIM(CDE_FLAG),
        DATANAV_ID = TRIM(DATANAV_ID),
        RULE_DESCRIPTION = TRIM(RULE_DESCRIPTION),
        DQ_DIMENSION = TRIM(DQ_DIMENSION),
        RULE_BEHAVIOR = TRIM(RULE_BEHAVIOR),
        RULE_TYPE = TRIM(RULE_TYPE),
        RULE_PRIORITY = TRIM(RULE_PRIORITY),
        THRESHOLD_TYPE = TRIM(THRESHOLD_TYPE),
        AMBER_THRESHOLD = TRIM(AMBER_THRESHOLD),
        RED_THRESHOLD = TRIM(RED_THRESHOLD),
        VARIANCE_TYPE = TRIM(VARIANCE_TYPE),
        CDE_ID = TRIM(CDE_ID)
    WHERE 1=1
    '''
    client.query(trim_query).result()

    print("Processing rules for table:", tables)
    tables_where_clause = ','.join(f"'{t}'" for t in tables)
    df = df[(df['DATA_TABLE'].isin(tables)) & (df['STATUS'] == 'Active')]

    update_snapshot_query_results = f'''
    UPDATE `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_RESULTS`
    SET is_latest_snapshot_flag = 'N'
    WHERE rule_id IN (
        SELECT rule_id FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`
        WHERE DATA_TABLE IN ({tables_where_clause})
    )'''
    client.query(update_snapshot_query_results).result()

    update_snapshot_query_execution = f'''
    UPDATE `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION_STATUS`
    SET is_latest_snapshot_flag = 'N'
    WHERE rule_id IN (
        SELECT rule_id FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`
        WHERE DATA_TABLE IN ({tables_where_clause})
    )'''
    client.query(update_snapshot_query_execution).result()

    results_rows_to_insert = []
    executions_rows_to_insert = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(process_rule, row): index for index, row in df.iterrows()}
        for future in as_completed(futures):
            index = futures[future]
            try:
                result, execution_status = future.result()
                if result:
                    for k, v in result.items():
                        if isinstance(v, Decimal):
                            result[k] = float(v)
                    results_rows_to_insert.append(result)
                executions_rows_to_insert.append(execution_status)
            except Exception as e:
                print(f"Exception in thread for row {index}: {e}")
                execution_status = {
                    'execution_id': str(uuid.uuid4()).split("-")[0],
                    'rule_id': df.iloc[index]['RULE_ID'],
                    'snapshot_date': date.today().strftime("%Y-%m-%d"),
                    'is_latest_snapshot_flag': 'Y',
                    'is_successful': 'N',
                    'error_message': str(e)
                }
                executions_rows_to_insert.append(execution_status)

    job = client.load_table_from_json(results_rows_to_insert, rules_results_table, job_config=job_config)
    job.result()
    job = client.load_table_from_json(executions_rows_to_insert, execution_status_table, job_config=job_config)
    job.result()

    print("Data inserted via batch loading")
