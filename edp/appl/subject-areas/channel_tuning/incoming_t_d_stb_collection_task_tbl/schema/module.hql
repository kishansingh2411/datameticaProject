--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Incoming table(incoming_nz_t_f_split_channel_tuning_rst_tbl)
--#   Author(s)   : DataMetica Team
--#   Usage       : beeline -u  <url> -n <username> -p <password> -hivevar var1=var1 -f module.hql
--#   Date        : 03/30/2016
--#   Log File    : ...log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
--#   SQL File    : 
--#   Error File  : .../log/channel_tuning/DEPLOYMENT_${current_timestamp}.log
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
   STB_COLLECTION_TASK_ID          INT     				COMMENT 'The unique identification of this table in the CVSTBP database. This is a system generated non-intrinsic number.',
   COLLECTION_TASK_IDENTIFICATION  INT     				COMMENT 'The generally accepted identification of the collection, per the business community',
   DTM_TO_START                    TIMESTAMP     		COMMENT 'The date/time the colletion is scheduled to start',
   DTM_TO_FINISH                   TIMESTAMP     		COMMENT 'The date/time the collection is scheduled to be completed',
   DTM_TASK_PUBLISHED              TIMESTAMP     		COMMENT 'The communicated date/time the collection was started from data collection.',
   DTM_TASK_SUSPENDED              TIMESTAMP     		COMMENT 'The communicated date/time the collection was suspended from data collection.',
   DTM_CREATED                     TIMESTAMP     		COMMENT 'date/time the row was inserted into the table.  Sourced from: SYSDATE',
   DTM_LAST_UPDATED                TIMESTAMP     		COMMENT 'date/time the row was last updated.  Sourced from SYSDATE.  Initially NULL when inserted.',
   SOURCED_FROM_SYSTEM             VARCHAR(31)     		COMMENT 'generally accepted name of the system the data was last updated.  hard coded.',
   LAST_UPDATED_BY_SYSTEM          VARCHAR(31)     		COMMENT 'generally accepted name of the system the data was last updated.  hard coded.  Initially set to NULL on an insert.',
   AGGREGATOR_STRING               VARCHAR(255)     	COMMENT 'A listing of aggregator acronyms, comma seperated.',
   PRE_CONDITION_STRING            VARCHAR(2000)     	COMMENT 'A string of conditions executed upon to derive or specify the households and/or devices to be included in the sample set.',
   PARAM_STRING                    VARCHAR(255)     	COMMENT 'parameters used in data collection utilized by the device for the collection characteristcs.  e.g. aggregator acronym, parameter, numeric value (the three nodes/parameters pairings may be iterated)',
   SAMPLE_DESCR                    VARCHAR(100)     	COMMENT 'Name by which the collection is generally known.  Free-form text.',
   SAMPLE_ID                       INT     				COMMENT '',
   TGT_DTM_CREATED                 TIMESTAMP     		COMMENT '',
   TGT_DTM_LAST_UPDATED            TIMESTAMP     		COMMENT '',
   TGT_SAMPLE                      VARCHAR(10)     		COMMENT '',
   FACT_LOAD_FLAG                  CHAR(1)     			COMMENT ''
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '^'
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################