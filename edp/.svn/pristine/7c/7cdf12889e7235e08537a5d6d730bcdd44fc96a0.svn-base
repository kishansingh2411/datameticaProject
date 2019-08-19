--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_ext_t_f_bm_dhcp_ip_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : .../log/HULU_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/HULU_DEPLOYMENT.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          03/30/2016       Initial version
--#
--#
--#####################################################################################################################

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(    
	 ACCOUNT_ID             DECIMAL(19,0)   COMMENT '',
	 HULU_ID                VARCHAR(128)    COMMENT '',
	 MAC                    VARCHAR(32)     COMMENT '',
	 HOME_ID                DECIMAL(16,0)   COMMENT '',
	 ACTION_TYPE            VARCHAR(32)     COMMENT '',
	 STATUS                 VARCHAR(32)     COMMENT '',
	 CREATE_DATE            VARCHAR(32)       COMMENT '',
	 SIGNUP_DATE            VARCHAR(32)       COMMENT '',
	 FIRST_CHARGE_DATE      VARCHAR(32)       COMMENT '',
	 CANCEL_DATE            VARCHAR(32)       COMMENT '',
	 LAST_UPDATED_DATE      VARCHAR(32)       COMMENT ''	 
)
PARTITIONED BY (SOURCE_DATE STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '~'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################