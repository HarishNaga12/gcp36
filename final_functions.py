
import pandas as pd
from google.cloud import bigquery
from datetime import datetime, timedelta, date
import numpy as np
from pandas.tseries.offsets import MonthEnd
from dataclasses import dataclass
import uuid
import time
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from decimal import Decimal

client = bigquery.Client()
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

# Load rules
rules = "SELECT * FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`"
query_job = client.query(rules)
df = query_job.result().to_dataframe()

# ----------------- Rule Validator -----------------
def validate_rule_inputs(rule_id, rule_behavior):
    rule_row = df[df['RULE_ID'] == str(rule_id)]

    if float(rule_row['AMBER_THRESHOLD'].values[0]) > float(rule_row['RED_THRESHOLD'].values[0]):
        raise ValueError(f"AMBER_THRESHOLD({rule_row['AMBER_THRESHOLD']}) cannot be greater than RED_THRESHOLD({rule_row['RED_THRESHOLD']}) for rule ID {rule_id}")

    if rule_behavior == 'Standard':
        amber = rule_row['AMBER_THRESHOLD'].values[0]
        red = rule_row['RED_THRESHOLD'].values[0]
        if pd.isnull(amber) or pd.isnull(red):
            raise ValueError(f"Standard check failed: NULL for AMBER_THRESHOLD or RED_THRESHOLD for rule ID {rule_id}")
    elif rule_behavior == 'Variance':
        process_days = rule_row['PROCESS_DAYS'].values[0]
        if pd.isnull(process_days):
            raise ValueError(f"Variance check failed: NULL for PROCESS_DAYS for rule ID {rule_id}")
    elif rule_behavior == 'P2P':
        denom = rule_row['DENOMINATOR_SQL'].values[0]
        if pd.isnull(denom):
            raise ValueError(f"P2P check failed: NULL for DENOMINATOR_SQL for rule ID {rule_id}")

# ----------------- Standard Check -----------------
def standard_check(rule_id):
    """Run the standard rule check.

    Returns
    -------
    tuple
        (rule_result, numerator_result, denominator_result, result_rag, error_message)
    """

    validate_rule_inputs(rule_id, 'Standard')
    threshold_type = df[df['RULE_ID'] == str(rule_id)]['THRESHOLD_TYPE'].values[0]

    error_message = None
    numerator_sql = df[df['RULE_ID'] == str(rule_id)]['NUMERATOR_SQL'].values[0]
    try:
        numerator_result = next(iter(client.query(numerator_sql).result()))[0]
    except Exception as e:
        numerator_result = None
        error_message = f"NUMERATOR_SQL error: {e}"

    if threshold_type.lower() == 'count':
        if numerator_result is None:
            return None, None, None, None, error_message or "NULL numerator"
        red_threshold = int(df[df['RULE_ID'] == str(rule_id)]['RED_THRESHOLD'].values[0])
        amber_threshold = int(df[df['RULE_ID'] == str(rule_id)]['AMBER_THRESHOLD'].values[0])
        result_rag = 'Red' if numerator_result >= red_threshold else 'Amber' if numerator_result >= amber_threshold else 'Green'
        return numerator_result, numerator_result, None, result_rag, None
    else:
        denominator_sql = df[df['RULE_ID'] == str(rule_id)]['DENOMINATOR_SQL'].values[0]
        denominator_result = None
        if numerator_result is not None:
            try:
                denominator_result = next(iter(client.query(denominator_sql).result()))[0]
            except Exception as e:
                error_message = f"DENOMINATOR_SQL error: {e}"

        if numerator_result is None or denominator_result in (None, 0):
            return None, numerator_result, denominator_result, None, error_message or "NULL denominator"

        percent_result = np.round(100 * (numerator_result / denominator_result), 2)
        red_threshold = int(df[df['RULE_ID'] == str(rule_id)]['RED_THRESHOLD'].values[0])
        amber_threshold = int(df[df['RULE_ID'] == str(rule_id)]['AMBER_THRESHOLD'].values[0])
        result_rag = 'Red' if percent_result >= red_threshold else 'Amber' if percent_result >= amber_threshold else 'Green'
        return percent_result, numerator_result, denominator_result, result_rag, None

# ----------------- Variance Check -----------------
def variance_check(rule_id):
    validate_rule_inputs(rule_id, 'Variance')
    numerator_sql = df[df['RULE_ID'] == str(rule_id)]['NUMERATOR_SQL'].values[0]
    numerator_result = next(iter(client.query(numerator_sql).result()))[0]
    N = int(df[df['RULE_ID'] == str(rule_id)]['PROCESS_DAYS'].values[0])
    test_query = f"""SELECT COUNT(*) FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde.STG_RULE_RESULTS`
                     WHERE rule_id = '{rule_id}' AND is_latest_snapshot_flag = 'N'"""
    number_of_records = next(iter(client.query(test_query).result()))[0]
    if number_of_records < N:
        return None, numerator_result, None, 'Green', None
    n_day_avg_sql = f"""WITH recent_n_records AS (
                          SELECT snapshot_date, numerator_result
                          FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde.STG_RULE_RESULTS`
                          WHERE rule_id = '{rule_id}' AND is_latest_snapshot_flag = 'N'
                          ORDER BY snapshot_date DESC LIMIT {N})
                        SELECT AVG(numerator_result) FROM recent_n_records"""
    n_average = round(float(next(iter(client.query(n_day_avg_sql).result()))), 2)
    if n_average is None:
        raise ValueError(f"Variance check failed: N-day AVG result is NULL for rule ID {rule_id}")
    percent_change = float(np.round(100 * abs(float(numerator_result) - float(n_average)) / float(n_average), 2))
    red_threshold = int(df[df['RULE_ID'] == str(rule_id)]['RED_THRESHOLD'].values[0])
    amber_threshold = int(df[df['RULE_ID'] == str(rule_id)]['AMBER_THRESHOLD'].values[0])
    result_rag = 'Red' if percent_change >= red_threshold else 'Amber' if percent_change >= amber_threshold else 'Green'
    return percent_change, numerator_result, n_average, result_rag, None

# ----------------- Point to Point Check -----------------
def ptp_check(rule_id):
    validate_rule_inputs(rule_id, 'P2P')
    threshold_type = df[df['RULE_ID'] == str(rule_id)]['THRESHOLD_TYPE'].values[0]
    numerator_sql = df[df['RULE_ID'] == str(rule_id)]['NUMERATOR_SQL'].values[0]
    numerator_result = next(iter(client.query(numerator_sql).result()))[0]
    if threshold_type.lower() == 'count':
        red_threshold = int(df[df['RULE_ID'] == str(rule_id)]['RED_THRESHOLD'].values[0])
        amber_threshold = int(df[df['RULE_ID'] == str(rule_id)]['AMBER_THRESHOLD'].values[0])
        result_rag = 'Red' if numerator_result >= red_threshold else 'Amber' if numerator_result >= amber_threshold else 'Green'
        return numerator_result, numerator_result, None, result_rag, None
    if numerator_result == 0:
        raise ValueError(f"P2P check failed: numerator result is ZERO for rule ID {rule_id} (division by zero risk)")
    denominator_sql = df[df['RULE_ID'] == str(rule_id)]['DENOMINATOR_SQL'].values[0]
    denominator_result = next(iter(client.query(denominator_sql).result()))[0]
    percent_result = np.round(100 * (denominator_result / numerator_result), 2)
    red_threshold = int(df[df['RULE_ID'] == str(rule_id)]['RED_THRESHOLD'].values[0])
    amber_threshold = int(df[df['RULE_ID'] == str(rule_id)]['AMBER_THRESHOLD'].values[0])
    result_rag = 'Red' if percent_result >= red_threshold else 'Amber' if percent_result >= amber_threshold else 'Green'
    return percent_result, numerator_result, denominator_result, result_rag, None

# ----------------- Execution Summary -----------------
def execution_status(tables):
    tables_list = ",".join(f"'{table}'" for table in tables)
    sql = f"""SELECT definition.DATA_TABLE as Table_Name,
                     COUNT(*) AS Total_Rules,
                     SUM(CASE WHEN execution.is_successful = 'Y' THEN 1 ELSE 0 END) AS Success_Rules,
                     SUM(CASE WHEN execution.is_successful = 'N' THEN 1 ELSE 0 END) AS Failed_Rules
              FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION` definition
              LEFT JOIN `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION_STATUS` execution
              ON execution.rule_id = definition.rule_id
              WHERE definition.DATA_TABLE in ({tables_list}) AND execution.is_latest_snapshot_flag = 'Y'
              GROUP BY definition.DATA_SCHEMA, definition.DATA_TABLE"""
    result = client.query(sql)
    rows = list(result)
    if not rows:
        return "<p>No Result found for CURRENT_DATE</p>"

    headers = rows[0].keys()
    html = "<table border='1'>\n<tr>" + "".join(f"<th>{header}</th>" for header in headers) + "</tr>\n"
    total_rules_executed = total_rules_passed = total_rules_failed = 0

    for row in rows:
        html += "<tr>" + "".join(f"<td>{row[header]}</td>" for header in headers) + "</tr>\n"
        total_rules_executed += row.get('Total_Rules', 0)
        total_rules_passed += row.get('Success_Rules', 0)
        total_rules_failed += row.get('Failed_Rules', 0)

    html += "</table>"
    html += "<br><h3>Overall Summary:</h3>"
    html += "<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>"
    html += "<tr><th>Metric</th><th>Value</th></tr>"
    html += f"<tr><td>Total Tables Processed</td><td>{len(rows)}</td></tr>"
    html += f"<tr><td>Total Rules Executed</td><td>{total_rules_executed}</td></tr>"
    html += f"<tr><td>Total Rules Passed</td><td>{total_rules_passed}</td></tr>"
    html += f"<tr><td>Total Rules Failed</td><td>{total_rules_failed}</td></tr>"
    html += "</table><br><p>Regards,<br><b>DQ_Table-DAG-test</b></p></body></html>"
    return html

# ----------------- Email Sender -----------------
def send_email(to_email, message_body):
    D = datetime.today().strftime('%Y-%m-%d')
    from_email = "Anthony_Moubarak@keybank.com"
    subject = "Data Quality -- Processed Tables"
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = from_email
    msg['To'] = ';'.join(to_email)
    body = MIMEText(message_body, 'html')
    msg.attach(body)
    smtpObj = smtplib.SMTP(host="10.16.88.17", port=25)
    smtpObj.sendmail(from_email, to_email, msg.as_string())
