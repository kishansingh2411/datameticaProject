--######################################################################################################################
--#   This source code is the property of:
--#   Cablevision Systems Corporation, Inc. (c) 2015
--#   1111 Stewart Avenue, Bethpage, NY 11714
--#   www.cablevision.com
--#   Department: AM
--#
--#   Program name: module.hql
--#   Program type: Hive Query Language script
--#   Purpose:    : Create Period table at Incoming layer
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
    DAY_TAG    	 				        CHAR(8),
    PERIOD_ID    				        INT,
    PERIOD_DATE  				        TIMESTAMP,
    YEAR         				        SMALLINT,
    QUARTER_NBR  				        TINYINT,
    MONTH_NBR    				        TINYINT,
    DAY_OF_THE_YEAR     			    SMALLINT,
    DAY_OF_MONTH        			    TINYINT,
    QUARTER_NBR_OVERALL 			    SMALLINT,
    MONTH_NBR_OVERALL   			    SMALLINT,
    WEEK_NBR_OVERALL    			    SMALLINT,
    DAY_NBR_OVERALL     			    INT,
    WEEK_NBR_IN_YEAR    			    TINYINT,
    DAY_OF_WEEK         			    VARCHAR(9),
    DAY_OF_WEEK_ABBR    			    CHAR(3),
    FISCAL_YEAR         			    SMALLINT,
    FISCAL_QUARTER      			    TINYINT,
    FISCAL_MONTH_NBR    			    TINYINT,
    FISCAL_MONTH_NBR_OVERALL  			SMALLINT,
    MONTH               			    VARCHAR(9),
    MONTH_ABBR          			    CHAR(3),
    FISCAL_QTR_OVERALL_TAG 			    CHAR(6),
    QUARTER_OVERALL_TAG  			    CHAR(6),
    MONTH_OVERALL_TAG    			    VARCHAR(7),
    LAST_DAY_IN_MONTH_IND  			    CHAR(1),
    LAST_DAY_OF_QUARTER_IND 			CHAR(1),
    US_HOLIDAY_IND        		 	    CHAR(1),
    CANADIAN_HOLIDAY_IND  			    CHAR(1),
    PREV_MONTH_START_PERIOD_ID  		INT,
    PREV_MTD_PERIOD_ID     			    INT,
    PREV_QTR_START_PERIOD_ID   			INT,
    PREV_QTD_PERIOD_ID     			    INT,
    PREV_YEAR_START_PERIOD_ID  			INT,
    PREV_YTD_PERIOD_ID      			INT,
    PREV_MONTH_PERIOD_ID    			INT,
    PREV_QUARTER_PERIOD_ID   			INT,
    PREV_YEAR_PERIOD_ID     			INT,
    MONTH_TAG        				    CHAR(6),
    DTM_CREATED      				    TIMESTAMP,
    DTM_LAST_UPDATED 				    TIMESTAMP,
    WEEK_NBR_OVERALL_MON_TO_SUN   		INT,
    WEEK_NBR_OVERALL_TUE_TO_MON   		INT,
    WEEK_NBR_OVERALL_WED_TO_TUE   		INT,
    WEEK_NBR_OVERALL_THU_TO_WED   		INT,
    WEEK_NBR_OVERALL_FRI_TO_THU   		INT,
    WEEK_NBR_OVERALL_SAT_TO_FRI   		INT,
    BROADCAST_YEAR                		SMALLINT,
    BROADCAST_MONTH_NBR_OF_YEAR   		TINYINT,
    BROADCAST_WEEK_NBR_OF_YEAR    		TINYINT,
    BROADCAST_DAY_NBR_OF_YEAR     		SMALLINT,
    BROADCAST_DAY_NBR_OF_WEEK     		TINYINT,
    BROADCAST_DAY_NBR_OF_MONTH    		TINYINT,
    BROADCAST_WEEK_DESC           		VARCHAR(30),
    CALENDAR_WEEK_DESC            		VARCHAR(30),
    BROADCAST_MONTH_DESC          		VARCHAR(15)   
)
ROW FORMAT DELIMITED 
   FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hivevar:location}'
TBLPROPERTIES ("skip.header.line.count"="1")
;

--############################################################################## 
--#                                    End                                     #
--##############################################################################