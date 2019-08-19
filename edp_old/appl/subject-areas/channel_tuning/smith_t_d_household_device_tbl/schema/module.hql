--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Gold table(gold_nz_t_f_split_channel_tuning_rst_tbl)
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
   household_device_id              INT              COMMENT '',
   household_key1                   VARCHAR(32)      COMMENT '',
   device_key1                      VARCHAR(32)      COMMENT '',
   device_key2                      VARCHAR(32)      COMMENT '',
   device_make                      VARCHAR(31)      COMMENT '',
   device_model                     VARCHAR(31)      COMMENT '',
   zip_code                         VARCHAR(10)      COMMENT '',
   sourced_from_system              VARCHAR(31)      COMMENT '',
   last_updated_by_system           VARCHAR(31)      COMMENT '',
   dtm_created                      TIMESTAMP        COMMENT '',
   dtm_last_updated                 TIMESTAMP        COMMENT '',
   ins_stb_file_control_id          INT               COMMENT '',
   upd_stb_file_control_id          INT               COMMENT '',
   created_by                       VARCHAR(30)       COMMENT '',
   household_type                   VARCHAR(2)        COMMENT '',
   household_type_descr             VARCHAR(20)       COMMENT '',
   household_state                  VARCHAR(2)       COMMENT '',
   household_state_descr            VARCHAR(20)       COMMENT '',
   last_updated_by                  VARCHAR(30)       COMMENT '',
   dtm_of_disconnect                TIMESTAMP         COMMENT '',
   disconnect_code                  CHAR(1)           COMMENT '',
   disconnect_descr                 VARCHAR(20)       COMMENT '',
   device_state                     VARCHAR(63)       COMMENT '',
   dtm_provisioned                  TIMESTAMP        COMMENT '',
   household_key2                   VARCHAR(32)       COMMENT '',
   household_key3                   VARCHAR(32)       COMMENT '',
   household_key4                   VARCHAR(32)      COMMENT '',
   household_key5                   VARCHAR(32)      COMMENT '',
   corp                             INT               COMMENT '',
   corp_franchise_tax_area          INT              COMMENT '',
   non_disclosure_ind               CHAR(1)          COMMENT '',
   dtm_of_non_disclosure_request    TIMESTAMP        COMMENT '',
   household_id                     INT              COMMENT '',
   corp_name                        VARCHAR(75)       COMMENT '',
   corp_franchise_tax_area_name     VARCHAR(50)       COMMENT '',
   household_key6                   VARCHAR(32)       COMMENT '',
   zip_code6                        VARCHAR(6)       COMMENT '',
   census                           VARCHAR(6)       COMMENT '',
   mdu                              VARCHAR(5)       COMMENT '',
   dtm_original_install             TIMESTAMP        COMMENT '',
   op_type                          CHAR(1)          COMMENT '',
   tgt_dtm_created                  TIMESTAMP        COMMENT '',
   tgt_dtm_last_updated             TIMESTAMP        COMMENT '',
   device_ext_key1                  VARCHAR(32)      COMMENT '',
   device_ext_key2                  VARCHAR(32)      COMMENT '',
   household_ext_key1               VARCHAR(32)      COMMENT '',
   household_ext_key2               VARCHAR(32)      COMMENT ''
)
STORED AS ORC
LOCATION '${hivevar:location}'
;

--##############################################################################
--#                                    End                                     #
--##############################################################################