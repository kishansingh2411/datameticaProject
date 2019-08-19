--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_t_f_bm_dhcp_ip_tbl)
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
	 CVAID                  STRING   COMMENT 'from deserializer',
	 HOUSEHOLD_KEY          STRING   COMMENT 'from deserializer',
	 CM_MAC_ADDR            STRING   COMMENT 'from deserializer',
	 IP_ADDR                STRING   COMMENT 'from deserializer',
	 CPE_MAC_ADDR           STRING   COMMENT 'from deserializer',
	 LEASE_TIME             STRING   COMMENT 'from deserializer',
	 TAG                    STRING   COMMENT 'from deserializer',
	 PROV_ACCT_FLG          STRING   COMMENT 'from deserializer',
	 OOL_RESI_FLG           STRING   COMMENT 'from deserializer',
	 DTM_CREATED            STRING   COMMENT 'from deserializer',
	 FILE_NAME              STRING   COMMENT 'from deserializer'
)
COMMENT 'This is NOC modem ip address data received every 6 hours filtered to tagComputer upstream'
PARTITIONED BY (SOURCE_DATE DATE)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################