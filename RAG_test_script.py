taw- DEV_RULE_RESULTS~
4=1 ; ;
result_rag = ‘Green’

d (rule_result >= amber_threshold or rule_result >=

centralrpt-prod-5796. centralrpt_dq_cde_dev.

red_threshold) 5

lways be > red threshold

lect count(*) as count

rs *alrpt-prod- -5796. -centralrpt dq_cde 1_ dev: DEV /_RULE_RESULTS*
where 1=1

and result Pag =-'Red’

2 @
rule_result < red_threshold;
Data elements |

PiGhanl 4,

d » 4 . ee hae
Sa ae Fe tea,
Dei rye a 1 + + odo Annan

All the _midae held he ainnan



Untitled

TA_E LEMENT_HEALTH™ |

en_rules != total_rul S3.

Check 2: If a rule is Amber,

it has to have more than 1 amber rule BUT @ red rules
select count(*) as count :

Staw- -centralrpt-prod-5796. *centralrpt_dq_cde_dev.DEV_DATA_ELEMENT_HEALTH

here data_element. rag = 'Amber'

and (red_rules != ® or amber_rules = saan

"select count(*) as count
om “taw-centralrpt-prod-5796

-centralrpt_dq_.
where data_element, ‘ag = 'Red'

cde_dev. DEV_DATA_ELEMENT_HEALTH*

‘### Table healt

# Check 1: When there is no table load health
Sa isnd

» Overall table health always
select count(*) as count

from ~taw- centralrpt- ~prod-

= table
where tl_table is null

-5796, “centralrpt_dq_ede_dev.DEV_TABLE HEALTH
and data_element —table_health |=

overall! table_health; UG
ee ind @0A7

Y%Duo in 29, col 13 Spaces: 4 |


it means total green rules = total rules

For table load health, if RAG is Amber there should be @ red rules and at fierce: 11
select count(*) as count

eooeralnaes prod-5796.centralrpt_ daicdas dev.DEV_TABLE_HEALTH™

.e_load_health = ‘Amber’
red_rules != @ or tl_amber_rules = @);'''

# Checl ‘4:For table load health, if RAG is Red there should be at least 1 ‘red rule
"select came as count

*Red'

Check 5: If oVerall table health is Green,

both data element rag and table load need to be Green
= ''" select count(*) as count
from ~taw- centralrpt-prod-5796, +centralrpt_dq_cde_dev.DEV_TABLE HEALTH*
Where 1=1 i
and tl_table is not null dL

: and overall _table_health = 'Green’
® 86

and (data _clement_table_health != 'Green’
ape ae

or table_load_health |=
575° Launchpad @0A7

"Green'); g, prea

Duo in 57, Col 58


--dq_cde_dev.DEV_TABLE_HEALTH™

and table_load_health != ‘Amber');

is Amber, data element rag and table load both cant be red

"alrpt_dq_cde_dev.DEV_TABLE_HEALTH*

or table_load_health = 'Red' 5

health is Red, at least one of data element rag and table load has to be red :
count

od-5796.centralrpt_dq_cde. dev.DEV_TABLE_HEALTH*
fealth = ‘Red’
ble_health != 'Red'

load_health != ‘Red’ ;

«
rules = [ri , r2 m3, dei, d
errors =[] 7 —.? Se?» de3, tha, th2, th3, tha, thS, thé, th7, tha]

for r in rule:
q16

ee @oner’ Peele = tenure

count] fon pew in pian my, Vl) mesnle(\1


m *taw-centralrpt-p

‘ta_element_table_health ;
sable_load_health != ‘Red’ 8

ules = [r1 , r2 , r3 , det, de2, de3, th1, th2, th3, tha, ths, thé, th7, ths]
ae :

for r in rules:
| query_result = [row["count"] for row in client.query(r).result()]

print("Error for rule:

—— append(int(query_result[e]))
if len(errors) == @:
~ print("No errors")

| af int(query result[e]) != @:

2°)

‘%Duo Ln 86, col 76 Spaces:4 UTF-8 CR


