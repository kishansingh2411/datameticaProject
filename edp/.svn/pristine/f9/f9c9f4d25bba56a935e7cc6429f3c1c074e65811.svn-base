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
#    	           1. Providing permission and changing files from Dos to Unix format		      
#                  2. Creating Local directories							                      
#  		           3. Creating HDFS directories for all data layer                              
#                  4. Creating Hive databases for all layers                                    
#                  5. Creating Hive Tables for all layers                                       
#                  6. Creating Metadata tables               
#   Author(s)   : DataMetica Team
#   Usage       : sh stock36_deployment.sh 
#   Date        : 09/12/2016
#   Log File    : .../log/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${job_name}.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          09/12/2016       Initial version
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

#######################################################################################
#                                                                                     #
#   Providing permission and changing files from Dos to Unix format 		          #
#                                                                                     #
#######################################################################################   

#chmod 755 -R $PROJECT_HOME
#find $SUBJECT_AREA_HOME -type f -exec dos2unix {} \;
chmod 755 -R $PROJECT_HOME
find $SUBJECT_AREAS_HOME -type f ! \( -name '*.jar' \) -exec dos2unix {} \;
find $PROJECT_HOME/bin/functions.sh -type f -exec dos2unix {} \;
find $PROJECT_HOME/etc/* -type f -exec dos2unix {} \;
find $PROJECT_HOME/rdbms_schema/* -type f -exec dos2unix {} \;

chmod 700 ${PROJECT_HOME}/etc/beeline.properties
chmod 700 ${PROJECT_HOME}/etc/postgres.properties

#######################################################################################
#                                                                                     #
#   Sourcing reference files  		                                                  #
#                                                                                     #
#######################################################################################   

source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/optimum_app.properties   

#######################################################################################
#                                                                                     #
#   Local Param 				 			     				                      #
#                                                                                     #
#######################################################################################   

log_file_path=$LOG_DIR_SUBJECT_AREA/OPT_APP_DEPLOYMENT.log

echo "Deployment scripts logs are generated at path $log_file_path"

#######################################################################################
#                                                                                     #
#   Creating Local directories 				 			     			              #
#                                                                                     #
#######################################################################################   

local_dirs=("${LOG_DIR_SUBJECT_AREA}" \
            "${BATCH_ID_DIR}")
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

hdfs_dirs=("${DATA_LAYER_DIR_WORK_SUBJECT_AREA}" \
           "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
           "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
           "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}" \
           "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_BADDATA}" \
           "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" \
           "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}" \
           "${DATA_LAYER_DIR_CACHE_SUBJECT_AREA}" \
           )
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

hive_databases=("${HIVE_DATABASE_NAME_INCOMING}" \
                "${HIVE_DATABASE_NAME_GOLD}" \
                "${HIVE_DATABASE_NAME_WORK}")
               
for hive_database in ${hive_databases[@]}
do
   date_layer_dir=''
   
   case ${hive_database} in
   
    ${HIVE_DATABASE_NAME_INCOMING})
	   date_layer_dir=${DATA_LAYER_DIR_INCOMING}
       ;;
       ${HIVE_DATABASE_NAME_GOLD})
	   date_layer_dir=${DATA_LAYER_DIR_GOLD}
       ;;
       ${HIVE_DATABASE_NAME_WORK})
	   date_layer_dir=${DATA_LAYER_DIR_WORK}
       ;;
    esac
	   
   fn_create_hive_database "${hive_database}" "${date_layer_dir}" "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${log_file_path}"
done

#######################################################################################
#                                                                                     #
#   Creating Hive Tables for all layers	         							          #
#                                                                                     #
#######################################################################################   
  
tables=($SUBJECT_AREA_HOME/incoming_*_tbl)
for table in "${tables[@]}"; do
    table_name=`basename ${table}`
    table_layer=`echo $table_name | cut -d'_' -f1`
    sh ${table}/bin/module.sh "setup" "${log_file_path}"
done         

tables=($SUBJECT_AREA_HOME/work_*_tbl)
for table in "${tables[@]}"; do
    table_name=`basename ${table}`
    table_layer=`echo $table_name | cut -d'_' -f1`
    sh ${table}/bin/module.sh "setup" "${log_file_path}"
done

tables=($SUBJECT_AREA_HOME/gold_*_tbl)
for table in "${tables[@]}"; do
    table_name=`basename ${table}`
    table_layer=`echo $table_name | cut -d'_' -f1`
    sh ${table}/bin/module.sh "setup" "${log_file_path}"
done

#######################################################################################
#                                                                                     #
#   Creating Metadata tables			                                              #
#                                                                                     #
#######################################################################################   


tables=($INCOMING_OPT_TAB_COLS_TBL \
        $INCOMING_OPTIMUM_SUITE_TBL \
        )
        	
for table in "${tables[@]}"; do  
    table_name=`basename ${table}`
    sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "prepare" "${log_file_path}"
    exit_code=$?
    
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to load incoming metadata table ${table_name}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
	fn_log_info "Successfully loaded incoming metadata table ${table_name}" "${log_file_path}"
	
done

#######################################################################################
#                                                                                     #
#   Copy remove_carriage.ksh file to HDFS location	                                  #
#                                                                                     #
####################################################################################### 

source_file_path=${SUBJECT_AREA_HOME}/common/bin/remove_carriage.ksh
Desination_path=${DATA_LAYER_DIR_CACHE_SUBJECT_AREA}


fn_copy_from_local "${source_file_path}" "${Desination_path}" \
"${BOOLEAN_FALSE}" "${BOOLEAN_FALSE}" "${log_file_path}"

exit_code=$?    
    
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
    	fn_log_error "For suite $suite_name, failed to move ksh file to cache, Quitting the prossess..!!!" "${log_file_path}"
  	    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
  	    
  	    exit $exit_code 
	fi

#######################################################################################
#                                                                                     #
#   Creating Postgres Sequence and Table			                                  #
#                                                                                     #
#######################################################################################   

export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE_NAME -a -f $PROJECT_HOME/rdbms_schema/postgres_schema.sql
exit_code=$?

success_message="Successfully created schema in Postgres"
failure_message="Failed while creating schema in Postgres"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${log_file_path}"

fn_log_info "Deployment scripts completed!!!" "${log_file_path}"

#######################################################################################
#                                    End                                              #
#######################################################################################
