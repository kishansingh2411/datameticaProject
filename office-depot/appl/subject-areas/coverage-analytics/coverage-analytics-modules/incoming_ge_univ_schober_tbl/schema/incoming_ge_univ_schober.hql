CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_GE_UNIV_SCHOBER}
(
COMPANY_ID	string,
NAME_1	string,
NAME_2	string,
NAME_3	string,
COUNTRY_CODE_VALUE	string,
ZIPCODE	string,
TOWN	string,
STREET	string,
PO_BOX	string,
ZIPCODE_PO_BOX	string,
TOWN_PO_BOX	string,
COMPANY_SALUTATION	string,
TEL_AREA_CODE	string,
TELEPHONE_NO	string,
FAX_AREA_CODE	string,
FAX_NO	string,
LEGAL_FORM	string,
COMPANY_SIZE	string,
COMPANY_TYPE	string,
YEAR_OF_FOUNDATION	string,
CHAMBER_OF_COMMERCE_NO	string,
NO_OF_EMPLOYEES	string,
TURNOVER	string,
POPULATION	string,
CAPITAL	string,
HOMEPAGE	string,
EMAIL	string,
MOBILE_PHONE_NO	string,
SERVICE_PHONE_NO	string,
REGION	string,
MAILORDER_ACTIVITY	string,
EMPLOYEE_CATEGORY	string,
ACTIVITY_CODE	string,
NO_WHITE_COLLAR_WORKERS	string,
NO_BLUE_COLLAR_WORKERS	string
)
PARTITIONED BY (batch_id string, country_code string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_GE_UNIV_SCHOBER}'
tblproperties ("skip.header.line.count"="1");
