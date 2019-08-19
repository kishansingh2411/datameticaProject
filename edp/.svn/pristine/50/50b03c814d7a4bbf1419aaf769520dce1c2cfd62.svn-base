#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: deployment.sh
#   Program type: Unix Bash Shell script
#   Purpose:    : Deployment script perform following activities
#                 1. SetUp ods Environment
#				  2. Providing permission and changing files from Dos to Unix format 
#       		  3. Creating Local directories 
#  		 		  4. Creating HDFS directories for all data layer
#        		  5. Creating Hive databases for all layers
#   Author(s)   : DataMetica Team
#   Usage       : sh deployment.sh
#   Date        : 02/01/2017
#   Log File    : .../log/ods/ODS_BHV_DEPLOYMENT.log
#   SQL File    : 
#   Error File  : .../log/ods/ODS_BHV_DEPLOYMENT.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          02/01/2017       Initial version
#
#
#####################################################################################################################

###############################################################################
#                          Module Environment Setup                           #
###############################################################################

# Find absolute path to this script which is used to define module, project, subject area home directory paths.
pushd . > /dev/null
SCRIPT_HOME="${BASH_SOURCE[0]}";
while([ -h "${SCRIPT_HOME}" ]); do
    cd "`dirname "${SCRIPT_HOME}"`"
    SCRIPT_HOME="$(readlink "`basename "${SCRIPT_HOME}"`")";
done
cd "`dirname "${SCRIPT_HOME}"`" > /dev/null
SCRIPT_HOME="`pwd`";
popd  > /dev/null

# Set module, project, subject area home paths.
SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

##############################################################################
#													                         # 
#   Providing permission and changing files from Dos to Unix format 		 #
#                                    									     #
##############################################################################   

chmod 755 -R $PROJECT_HOME
find $SUBJECT_AREAS_HOME -type f ! \( -name '*.jar' \) -exec dos2unix {} \;
find $PROJECT_HOME/bin/functions.sh -type f -exec dos2unix {} \;
find $PROJECT_HOME/etc/* -type f -exec dos2unix {} \;
find $PROJECT_HOME/rdbms_schema/* -type f -exec dos2unix {} \;

chmod 600 $PROJECT_HOME/etc/beeline.properties
chmod 600 $PROJECT_HOME/etc/postgres.properties
chmod 600 $SUBJECT_AREA_HOME/etc/oracle.properties

##############################################################################
#													                         # 
#   Sourcing reference files  		                                         #
#													                         # 
############################################################################## 

source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/ods.properties

##############################################################################
#													                         # 
#   Local Param 				 			     				             #
#													                         # 
##############################################################################   

current_timestamp=`date +%Y%m%d%H%M%S`
log_file_path=$LOG_DIR_SUBJECT_AREA/ODS_BHV_DEPLOYMENT.log
fail_on_error="${BOOLEAN_TRUE}"

echo "Deployment scripts logs are generated at path $log_file_path"

##############################################################################
#													                         # 
#   Creating Local directories 				 			     			     #
#													                         # 									
##############################################################################         

local_dirs=("${LOG_DIR_SUBJECT_AREA}" \
            "${BATCH_ID_DIR_SUBJECT_AREA}" \
           )
for local_dir in "${local_dirs[@]}"
do
   fn_local_create_directory_if_not_exists "${local_dir}" "${BOOLEAN_FALSE}" "${log_file_path}"
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to create local directory ${local_dir}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
done

#######################################################################################
#                                                                                     #
#   Creating HDFS directories for all data layer 				 			          #
#                                                                                     #
#######################################################################################   

hdfs_dirs=("${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_CUSTOMER}" \
           "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}" \
           "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA_CUSTOMER}")
for hdfs_dir in "${hdfs_dirs[@]}"
do
   fn_hadoop_create_directory_if_not_exists ${hdfs_dir} ${BOOLEAN_FALSE} ${log_file_path}
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to create HDFS directory ${hdfs_dir}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
done

#######################################################################################
#                                                                                     #
#   Creating Hive databases for all layers	         					              #
#                                                                                     #
#######################################################################################   

hive_databases=("${HIVE_DATABASE_NAME_INCOMING_ODS}" \
                 "${HIVE_DATABASE_NAME_GOLD_ODS}" \
                 "${HIVE_DATABASE_NAME_WORK_ODS}")
                
for hive_database in ${hive_databases[@]}
do
   date_layer_dir=''
   
   case ${hive_database} in
   
    ${HIVE_DATABASE_NAME_INCOMING_ODS})
	   date_layer_dir=${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}
       ;;
    ${HIVE_DATABASE_NAME_WORK_ODS})
	   date_layer_dir=${DATA_LAYER_DIR_WORK_SUBJECT_AREA}
       ;;
    ${HIVE_DATABASE_NAME_GOLD_ODS})
	   date_layer_dir=${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}
       ;;
    esac
	   
   fn_create_hive_database "${hive_database}" "${date_layer_dir}" "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${log_file_path}"
done

##############################################################################
#													                         #  
#   Creating Hive Tables for all layers	         							 # 
#                                    									     #
##############################################################################

tables=("${INCOMING_ACCOUNT_INFO_TBL}_tbl" \
        "${INCOMING_ACCOUNT_TNS_TBL}_tbl" \
        "${INCOMING_CUST_TELEPHONE_NBR_TBL}_tbl" \
        "${INCOMING_CUSTOMER_TELEPHONE_TBL}_tbl" \
        "${GOLD_BHV_ACCT_PHONE_NBR_OPT_TBL}_tbl" \
        "${GOLD_BHV_ACCT_PHONE_NBR_SDL_TBL}_tbl" \
        "${GOLD_BHV_ACCT_PHONE_NBR_OPT_SDL_TBL}_tbl" \
        "${WORK_BHV_ACCT_PHONE_NBR_OPT_TBL}_tbl" \
        "${WORK_BHV_ACCT_PHONE_NBR_OPT_TMP_TBL}_tbl" \
        "${WORK_BHV_ACCT_PHONE_NBR_SDL_TBL}_tbl" \
        "${WORK_BHV_ACCT_PHONE_NBR_SDL_TMP_TBL}_tbl")

for table in "${tables[@]}"; do  
    sh "${SUBJECT_AREA_HOME}/${table}/bin/module.sh" "setup" "${log_file_path}"
done

fn_log_info "ODS Deployment as part of OVCDR enhancement scripts completed!!!" "${log_file_path}"

##############################################################################
#                                    End                                     #
##############################################################################