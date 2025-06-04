STIG_RU Xx
3 Noteb X JailyR: X

Works! X | © Conne x

3ca918e65c4-dot-us-central1.notebo:

Learnings Boards in Ente

rom google.cloud import

A
|
def dailyrun():,
client = bigquery.
# Step 1 - Refreshed Table Names
query =
SELECT TABLE_NAME

FROM ~ taw-centralept-prod-5796.centralrpt_dq_cde.STG_SRC_TABLE_RE! FRESH_METADATA™
WHERE REFRESHED _TODAY_FLAG’= 'y' 7

BSwvavaupunr

Stepi_result = [row["TABLE_NAME"].strip() for row in client.query(query).result()]

43 #print("\nStep 1 - Refreshed Tables”)
14 #step] result = [row["TABLE_NAME"] for row in client.query(query).result()]
4s #print(f"Refreshed Tables: {stepl_result}")

v7 # Step 2 - Rules Processed Today
18 query =
19 SELECT data_table

20 FROM > taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_TABLE_RULE_EXECUTIONS™

21 WHERE table_processed_today = ‘Y’ ‘

23 Step2_result = [row["data_table"].strip() for row in client.query(query)-result()]

print("\n--- Step1 vs Step 2 - Validation Rules processed: ")
set(Step1_result)== set(Step2_result):

27 print(#"Match: {len(Step1_result)} tables refreshed. and processed")
28. else: é
29 print("Mismatch:")

30 print(f"STG_SRC_TABLE_REFRESH_METADATA refreshed {len(Step1_result)} “)
'€"DEV_TABLE_RULE_EXECUTIONS Processed {len(Step2_result)} ")

31
32
33 # Step.3 - Rule Definition Count ‘ : tors

| = query =

35 SELECT COUNT(*) AS rule_def_count
z — Ln2,Col1 Spaces:



processing py
nepy
Py

AND is_latest_snapshot =

int(#"Table_rules_count: {Table rules_count}")

Total Elenemnt rules rules in Rule Results
Eeery oleae

SELECT sum(total rules) as DE_rules_count FROM > taw-centralrpt-prod-5796.centralrpt_dq_cde_dév.DEV_DATA_ELEMENT_HEALTH

“where snapshot_date = CURRENT_DATE() 5
AND is_latest_snapshot = ‘y’

DE_rules_count = list(client.query(query). result())[@]["DE_rules_count"]
print(f"DE rules count: {DE_rules_count}"

total_rules count= Table_rules_count+DE_rules_count
prant(#"total rules count from TH& DE: {total_rules_count}")
print(fLogged rule _results_count: {rule results_count}")

total_rules_count == rule_results_count:
pr

(#" PASS: Table_rules_count& DE_rules_count is equeal to Rule_results")

rint(" PASS: Table_rules_count& DE_rules count is equeal to Rule_results")
if _name_=='_main_':
dailyrun()

Table _rules_count = list(client.query(queryt).result())[0]["Table_rules_count"]

1n 176, Col 33 Spaces: 4



ng.py 23 days ago

= DailyRunpy

| HEALTH ---")
ain client.query(query).result()]
t)}")

# Step 7b - Table-leve
query = """ ;
SELECT DISTINCT data_tal r
FROM ~taw-centralrpt-prod- ralrpt_dq_cde_dev.DEV_RULE_DEFINITION™
WHERE rule id IN ( 4
SELECT rule id ‘

FROM “taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION_STATUS”
WHERE is_latest_snapshot_flag = 'Y'

AND snapshot_date = CURRENT_DATE()

print("\n--- Step 7b - Tables from RULE_DEFINITION (Table-level only) ---")
Part_b result = [row["data table”] for row in client.query(query).result()]
print(#"7b-Part B Tables: {len(part_b_result)}")

Step 7 Validation: Part A vs Part B")

Gart_a result) == set(part_b result) and {len(part_a_result)} == {len(part_b_result)}:
CF" Match: {len(part_a_result)} tables in both Part A and Part B")

print(#" Mismatch:")
print(: Part A Tables: {sorted(part_; result)}"
print(f"- Part B Tables: {sorted(part_b_result)}")

ictiidiiidiiadaE LLL
print("\n--- Step 8 - Total Sum of ru
#Total Table rules in Rule Results

gueryt =
SELECT .sum(t1_total
where snapshot_date

les in Table Health & Element_Health Table ---")

rules) as Table_rules_count FROM “taw-centralnpt-prod-5796. centralrpt_dq_cde_dev.DEV_TABLE HEALTH
= CURRENT_DATE() .


| # Step 6b - Rule Definit
query = """
SELECT DISTINCT DAT/
FROM ~taw-centralrpt-
WHERE rule_id IN (
SELECT rule_id
FROM ~taw-central,
WHERE is _latest_snay
. AND snapshot_date
)
AND DATA_COLUMN IS NOT NULL
and status = ‘Active’

print(™\n—— Step 6b - DE-DATA_COLUMN from RULE_DEFINITION & for rule_id of Execution_results —-
Part_b result = [row[“DATA_COLUMN"] for row in client. query(query).result()]
print(#"6b-Part B Columns: {len(part_b_result)}") .

Print("\nStep 6 Validation: Part A vs Part B")
af set(part_a result) == set(part_b_result) and {len(part_a_result)} == {len(part_b_result)} =

print(#" Match: {len(part_a_result)} columns in both Part A and Part B  {len(part_b result)}"
else: . . : es

print(#" Mismatch:") . 2
print(#"- Part A Columns: {len(part_a_result)}")
print(#"- Part B Columns: {len(part_b _result)}")

# Step 7a - Table Health

query = “""

SELECT data table

FROM ”taw-centralrpt-prod-5796.c
WHERE is latest_snapshot = 'y’
AND 'snapshot_date = CURRENT_DATE()

entralrpt_dq_cde_dev.DEV_TABLE_HEALTH™



“Tse

“snapshot_date = CURRENT DAT
AND is_successful = 'y"

successful_execution_count = List r t()) [2] ["execution, _counte]

print(f"\n-- Step 5a - Successfull Ex y successful. |_execution_count}") ‘

) # Step Sb - Rule Results Logged
query = ~""
SELECT COUNT(*) AS result_count
~ FROM“ taw-centralrpt-prod-5796.centralrpt vif dev . DEV. /_RULE_RESULTS"
WHERE is_latest_snapshot flag

=a
AND snapshot_date = CURRENT_DATE()

_ rule_results_count = list!

r (client .query(query).result()) [0] ["
print (F*

result_count"]
~~ Step 5b - Rule Results Logged Count: {rule_results_count}")
print("\nstep 5 Validation")
«if successfull i_count == rule results count:
print(f"
bia else:

Match: {successful_execution 1_count} successful executions and {rule_results_count} rule results logged
lee print(f"
1

Mismatch: {successful_execution_count} successful executions vs {rule _results s_count} rule i logged”

# Step 6a - Data Element Health
query = """

SELECT DATA_COLUMN, |,pde_id

FROM fo settralnpt-prod-5796. centralnpe_dq_ede_dev.DEV_DATA_ELEMENT HEALTH:
WHERE is _latest_snapshot = ‘y'
AND snapshot_date = CURRENT_DATE()

print("\n--- Step 6a - DATA_COI

Part_a_result = [row{"DATA_COLI

LUNN from DATA_ELEMENT_HEALTH ---")
Print(#"6a-Part A Columns:

UMN" ] for pow in client. query(query) result()]
{len(part_a a_result)}")



ercontent.com.

® Learnings Boards in

ae ero 5796. centralrpt dq_cde_dev.DEVRULE_DEFINITION™
IN

p
t=prod-5796.centralrpt_< dq_cde_dev.DEV_TABLE-RULE EXECUTIONS”
essed today = 'y'

"Rule Definition Count"

" rulle_def_count = List (client.query(query).result())[0]["rule def
print(f*"Rule Definition Count: {rule def count}")

# Step 4 - Rules Executed Today
a9 query = """

7) SELECT COUNT(*) AS execution_count
51 FROM ~taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION. STATUS”
52 WHERE is _latest_snapshot_flag = 'Y'

53 AND snapshot_date = CURRENT_DATE()

Eo 5

55 print("\nStep 4 - Rule Execution Count")

56 execution_count = list(client.query(query).result())[0]["execution_count"]
57 print(f"Rules Executed Today: {execution_count}") 5

59 print("\nStep 3(Rule Definition) vs Step 4(Rule Execution) Validation ---")

6 if rule_def_count == execution_count:

61 print(f" Match: {rule_def_count} rules defined and {execution_count} executed")
62 else

63 print(f" Mismatch: {rule_def_count} gules defined vs {execution_count} executed")

66 # Step 5a - Successful Executions

67 query = """

| 68 SELECT COUNT(*) AS execution_count

i 69 FROM ~taw-centralrpt-prod-5796.centralrpt_dq_cde_dev.DEV_EXECUTION_STATUS|
| 70 WHERE is_latest_snapshot_flag = 'Y'



