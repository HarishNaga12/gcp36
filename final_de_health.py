from google.cloud import bigquery

client = bigquery.Client()
client.get_table('taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_DATA_ELEMENT_HEALTH')
job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")

def de_health(tables):
    tables_where_clause = ', '.join(f"'{t}'" for t in tables)  # create the string where clause from the tables array

    de_script = f"""
    with data_elements as (
        select rules.DATANAV_ID as pde_id, rules.DATA_SOURCE, rules.DATA_SCHEMA, rules.DATA_TABLE, rules.DATA_COLUMN, results.rule_id ,
               results.snapshot_date , results.result_rag
        from `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_RESULTS` results
        left join `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_RULE_DEFINITION` rules
        on results.rule_id = rules.rule_id
        where results.rule_level = 'Data Element'
        and results.is_latest_snapshot_flag = 'Y'
        and rules.DATA_TABLE in ({tables_where_clause})
    ), processed_rag as (
        select snapshot_date , pde_id, data_source , data_schema , data_table , data_column ,
               sum(case when result_rag = 'Amber' then 1 else 0 end) as amber_rules,
               sum(case when result_rag = 'Red' then 1 else 0 end) as red_rules,
               count(*) as total_rules
        from data_elements
        group by 1,2,3,4,5,6
    ), green as (
        select *,
               'Y' as is_latest_snapshot,
               case
                   when red_rules >= 1 then 'Red'
                   when amber_rules >= 1 then 'Amber'
                   else 'Green'
               end as data_element_rag
        from processed_rag
    )

    select * from green
    """

    update_snapshot_query = f"""
    update `taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_DATA_ELEMENT_HEALTH`
    set is_latest_snapshot = 'N'
    where data_table in ({tables_where_clause})
    """

    update_snapshot_query_job = client.query(update_snapshot_query)
    update_snapshot_query_result = update_snapshot_query_job.result()

    de_job = client.query(de_script)
    results = de_job.result()

    de_rows = [[k.isoformat() if hasattr(v, 'isoformat') else v for k,v in dict(row).items()] for row in results]

    de_health_table = 'taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_DATA_ELEMENT_HEALTH'

    try:
        if de_rows:
            load_job = client.load_table_from_json(
                de_rows,
                de_health_table,
                job_config=job_config
            )
            load_job.result()
            print("Data inserted via batch loading.")
    except Exception as e:
        print("Batch load failed:", e)
