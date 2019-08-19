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
--#   Date        : 12/28/2015
--#   Log File    : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
--#   SQL File    : 
--#   Error File  : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
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

DROP TABLE IF EXISTS ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name};

CREATE EXTERNAL TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(    
	 CVAID                  STRING   COMMENT '',
	 HOUSEHOLD_KEY          STRING   COMMENT '',
	 CM_MAC_ADDR            STRING   COMMENT '',
	 IP_ADDR                STRING   COMMENT '',
	 CPE_MAC_ADDR           STRING   COMMENT '',
	 LEASE_TIME             STRING   COMMENT '',
	 TAG                    STRING   COMMENT '',
	 PROV_ACCT_FLG          STRING   COMMENT '',
	 OOL_RESI_FLG           STRING   COMMENT '',
	 LOAD_DATE              STRING   COMMENT '',
	 DTM_CREATED            STRING   COMMENT '',
	 FILE_NAME              STRING   COMMENT ''
)
PARTITIONED BY (SOURCE_DATE DATE,
                HH STRING)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY '|'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################