--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create table work_chc_int_rad_call at Work layer.
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 12/28/2016
--#   Log File    : .../log/ovcdr/${job_name}.log
--#   SQL File    : 
--#   Error File  : .../log/ovcdr/${job_name}.log
--#   Dependency  : 
--#   Disclaimer  : 
--#
--#   Modification History :
--#   =======================
--#
--#    ver     who                      date             comments
--#   ------   --------------------     ----------       -------------------------------------
--#    1.0     DataMetica Team          12/28/2016       Initial version
--#
--#
--#####################################################################################################################

CREATE TABLE ${hivevar:database}.${hivevar:table_prefix}${hivevar:table_name}
(   
  USAGE_DATE         CHAR(8) COMMENT '', 
  TELEPHONE_NUMBER   VARCHAR(10) COMMENT '',
  CORP               VARCHAR(6) COMMENT '',
  HOUSE              VARCHAR(6) COMMENT '',
  CUST               VARCHAR(2) COMMENT ''
)
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################