from google.cloud import bigquery

client = bigquery.Client()
client.get_table('taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_HEALTH')
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

def tl_health(tables):
    tables_where_clause = ', '.join(f"'{t}'" for t in tables)

    th_script = f"""
    with data_element_table_health AS (
        SELECT
          data_source,
          data_schema,
          data_table,
          MAX(snapshot_date) AS snapshot_date,
          CASE MIN(
            CASE
              WHEN LOWER(data_element_rag) = 'red' THEN 1
              WHEN LOWER(data_element_rag) = 'amber' THEN 2
              ELSE 3
            END
          )
          WHEN 1 THEN 'Red'
          WHEN 2 THEN 'Amber'
          ELSE 'Green'
          END AS data_element_table_health
        FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_DATA_ELEMENT_HEALTH`
        WHERE is_latest_snapshot = 'Y'
        AND data_table IN ({tables_where_clause})
        GROUP BY 1,2,3
        ORDER BY 1
    ),
    data_elements AS (
        SELECT rules.DATA_SOURCE, rules.DATA_SCHEMA, rules.DATA_TABLE, rules.rule_id, 
               results.snapshot_date, results.result_rag
        FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_RESULTS` results
        LEFT JOIN `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION` rules
        ON results.rule_id = rules.rule_id
        WHERE results.rule_level = 'Table'
        AND results.is_latest_snapshot_flag = 'Y'
        AND rules.DATA_TABLE IN ({tables_where_clause})
    ),
    processed_rag AS (
        SELECT snapshot_date, data_source, data_schema, data_table,
               SUM(CASE WHEN result_rag = 'Green' THEN 1 ELSE 0 END) AS green_rules,
               SUM(CASE WHEN result_rag = 'Amber' THEN 1 ELSE 0 END) AS amber_rules,
               SUM(CASE WHEN result_rag = 'Red' THEN 1 ELSE 0 END) AS red_rules,
               COUNT(*) AS total_rules
        FROM data_elements
        GROUP BY 1,2,3,4
    ),
    table_load_health AS (
        SELECT
          'Y' AS is_latest_snapshot,
          CASE
            WHEN red_rules >= 1 THEN 'Red'
            WHEN amber_rules >= 1 THEN 'Amber'
            ELSE 'Green'
          END AS table_rag
        FROM processed_rag
    ),
    all_tables AS (
        SELECT de.*, t.table_rag AS tl_table, t.green_rules, t.red_rules, t.amber_rules,
               t.total_rules AS tl_total_rules
        FROM data_element_table_health de
        LEFT JOIN table_load_health t ON de.data_table = t.data_table
    )
    SELECT all_tables.*, 'Y' AS is_latest_snapshot,
           CASE
             WHEN all_tables.tl_table IS NULL THEN all_tables.data_element_table_health
             WHEN all_tables.tl_table = 'Red' THEN 'Red'
             WHEN all_tables.tl_table = 'Amber' THEN 
               CASE WHEN data_element_table_health = 'Red' THEN 'Red' ELSE 'Amber' END
             ELSE
               CASE
                 WHEN data_element_table_health = 'Red' THEN 'Red'
                 WHEN data_element_table_health = 'Amber' THEN 'Amber'
                 ELSE 'Green'
               END
           END AS overall_table_health,
           drd.datanav_id AS table_id
    FROM all_tables
    LEFT JOIN (
        SELECT DISTINCT DATA_SOURCE, DATA_SCHEMA, DATA_TABLE, DATANAV_ID
        FROM `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION`
        WHERE DATA_COLUMN IS NULL
    ) drd
    ON all_tables.data_source = drd.data_source
    AND all_tables.data_schema = drd.data_schema
    AND all_tables.data_table = drd.data_table
    """

    update_snapshot_query = f"""
    UPDATE `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_HEALTH`
    SET is_latest_snapshot = 'N'
    WHERE data_table IN ({tables_where_clause})
    """
    update_snapshot_query_job = client.query(update_snapshot_query)
    update_snapshot_query_result = update_snapshot_query_job.result()

    th_job = client.query(th_script)
    results = th_job.result()
    th_rows = [[v.isoformat() if hasattr(v, 'isoformat') else v for k,v in dict(row).items()] for row in results]

    th_table = 'taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_HEALTH'
    try:
        if th_rows:
            load_job = client.load_table_from_json(th_rows, th_table, job_config=job_config)
            load_job.result()
            print("Data inserted via batch loading")
    except Exception as e:
        print("Batch load failed:", e)
