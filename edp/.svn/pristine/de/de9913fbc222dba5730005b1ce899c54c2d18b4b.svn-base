--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Optimum_suite table at Incoming layer
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2015
--#   Log File    : .../log/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2015       Initial version
--#
--#
--#####################################################################################################################


CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(
    SUITE_ID 	              INT 			COMMENT 'Omniture ID (Onet.prod-1, ESP-2)',
    SUITE_NAME                VARCHAR(100) 	COMMENT 'Name of Suite',
    SUITE_DESCRIPTION         VARCHAR(100) 	COMMENT 'Description of Suite',
    OMNITURE_FILE_NAME 	      VARCHAR(60) 	COMMENT 'Omniture File name',
    OMNITURE_FTP_ADDRESS      VARCHAR(60) 	COMMENT 'FTP address of Omniture',
    DTM_START 	              DATE 			COMMENT 'Start Date',
    DTM_END                   DATE 			COMMENT 'End Date',
    DTM_CREATED               TIMESTAMP 	COMMENT 'Date and Time the record was created',
    DTM_UPDATED               TIMESTAMP 	COMMENT 'Sourced from the system (Omniture)'
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
tblproperties ("skip.header.line.count"="1")
;

--##############################################################################
--#                                    End                                     #
--##############################################################################