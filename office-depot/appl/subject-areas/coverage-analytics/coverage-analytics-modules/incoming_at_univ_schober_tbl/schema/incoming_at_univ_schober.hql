CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:DB_INCOMING}.${hiveconf:TBL_INCOMING_AT_UNIV_SCHOBER}
(
LAST_NAME string,
COMPANY_NAME_1 string,
COMPANY_NAME_2 string,
COMPANY_NAME_3 string,
STREET string,
STREET_POSTALCODE string,
TOWN_PO_BOX string,
PO_BOX string,
PO_BOX_POSTALCODE string,
PO_BOX_CITY string,
PHONE_AREA_CODE string,
PHONE_NUMBER string,
FAX_AREA_CODE string,
FAX_NUMBER string,
TYPE_OF_COMPANY string,
LEGAL_FORM string,
NO_OF_EMPLOYEES string,
COMPANY_SIZE string,
NO_OF_BEDS string,
NO_CHAMBER_OF_COMMERCE string,
YEAR_FOUNDATION string,
BUSINESS_CAPITAL string,
TURNOVER string,
SIZE_OF_CITY string,
EBC_CODE string,
EMAIL string,
HOMEPAGE string,
SHARED_PRACTICE string,
MOBILPHONE string,
TYP_WGS84 string,
X_GEO_WGS84 string,
Y_GEO_WGS84 string,
TYP_LAMBERT string,
X_GEO_LAMBERT_KONF string,
Y_GEO_LAMBERT_KONF string,
NACE_2008 string,
NACE_2008_DESC string,
BLUE_COLLAR_WORKER string,
WHITE_COLLAR_WORKER string,
GROWTH_INDICATOR string,
FLAG_SCORE string
)
PARTITIONED BY (batch_id  string, country_code  string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LOCATION '${hiveconf:INCOMING_HDFS}/${hiveconf:TBL_INCOMING_AT_UNIV_SCHOBER}'
tblproperties ("skip.header.line.count"="1");
