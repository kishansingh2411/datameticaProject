CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_AT_UNIV_KSV}
(
HEROLD_ID string,
KSV_ID string,
NAME string,
STREET string,
ZIPCODE string,
CITY string,
NO_OF_EMPLOYEES string,
NO_WHITE_COLLAR_WORKERS string,
NO_BLUE_COLLAR_WORKERS string,
COMPANY_NAME string,
COMPANY_NAME_KSV string,
UID_NO string,
TELEPHONE_1 string,
TELEPHONE_2 string,
TELEPHONE_3 string,
TELEPHONE_4 string,
TELEPHONE_5 string,
E_MAIL_1 string,
E_MAIL_2 string,
E_MAIL_3 string,
E_MAIL_4 string,
E_MAIL_5 string,
CAT_BRANCH_GROUP_1 string,
CAT_BRANCH_GROUP_2 string,
CAT_BRANCH_GROUP_3 string,
CAT_BRANCH_GROUP_4 string,
CAT_BRANCH_GROUP_5 string,
AT_NACE_BRANCH_NO_1 string,
AT_NACE_BRANCH_NO_2 string,
AT_NACE_BRANCH_NO_3 string,
AT_NACE_BRANCH_NO_4 string,
AT_NACE_BRANCH_NO_5 string,
AT_NACE_BRANCH_1 string,
AT_NACE_BRANCH_2 string,
AT_NACE_BRANCH_3 string,
AT_NACE_BRANCH_4 string,
AT_NACE_BRANCH_5 string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_AT_UNIV_KSV}'
tblproperties ("skip.header.line.count"="1");
