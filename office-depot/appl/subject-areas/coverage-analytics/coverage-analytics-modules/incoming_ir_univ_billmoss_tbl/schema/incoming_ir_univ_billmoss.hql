CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_IR_UNIV_BILLMOSS}
(
 SITEID Float,
SITE_NAME string,
ADD1 string,
ADD2 string,
ADD3 string,
ADD4 string,
ADD5 string,
ADD6 string,
EMPLOYEES Float,
SIC1987_4DIGIT string,
CO_UID Float,
SUSPEND string
 )
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_IR_UNIV_BILLMOSS}'
tblproperties ("skip.header.line.count"="1");
