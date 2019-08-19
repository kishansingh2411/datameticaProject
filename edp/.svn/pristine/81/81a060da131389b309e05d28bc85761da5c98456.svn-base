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
#                 1. SetUp bering_media Environment											  
#  		          2. Create Hive tables                                						  
#                 3. Create postgres tables  
#   Author(s)   : DataMetica Team
#   Usage       : sh deployment.sh
#   Date        : 12/28/2015
#   Log File    : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
#   SQL File    : 
#   Error File  : .../log/bering_media/BERING_MEDIA_DEPLOYMENT.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          12/28/2015       Initial version
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
#   Providing permission and changing files from DOS to Unix format 		          #
#                                                                                     #
#######################################################################################   

chmod 755 -R $PROJECT_HOME
find $SUBJECT_AREAS_HOME -type f -exec dos2unix {} \;
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
source $SUBJECT_AREA_HOME/etc/bering-media.properties


#######################################################################################
#                                                                                     #
#   Local Param 				 			     				                      #
#                                                                                     #
#######################################################################################   

current_timestamp=`date +%Y%m%d%H%M%S`
log_file_path=$LOG_DIR_SUBJECT_AREA/BERING_MEDIA_DEPLOYMENT.log

#######################################################################################
#                                                                                     #
#   Creating Local directories 				 			     			              #
#                                                                                     #
#######################################################################################   

local_dirs=("${LOG_DIR_SUBJECT_AREA}" \
            "${BATCH_ID_DIR_SUBJECT_AREA}")
for local_dir in "${local_dirs[@]}"
do
   fn_local_create_directory_if_not_exists "${local_dir}" "${BOOLEAN_TRUE}" "${log_file_path}"
done

fn_log_info "Deployment scripts logs are generated at path $log_file_path" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating HDFS directories for all data layer 				 			          #
#                                                                                     #
#######################################################################################   

hdfs_dirs=("${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_PROCESSING}" \
           "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}" \
           "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
           "${DATA_LAYER_DIR_ARCHIVE_SUBJECT_AREA}" \
          )
for hdfs_dir in "${hdfs_dirs[@]}"
do
   fn_hadoop_create_directory_if_not_exists ${hdfs_dir} ${BOOLEAN_TRUE} ${log_file_path}
done

#######################################################################################
#                                                                                     #
#   Creating Hive databases for all layers	         					              #
#                                                                                     #
#######################################################################################   

hive_databases=("${HIVE_DATABASE_NAME_INCOMING}" \
			    "${HIVE_DATABASE_NAME_GOLD}")
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
    esac
   fn_create_hive_database "${hive_database}" "${date_layer_dir}" "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${log_file_path}"
done

#######################################################################################
#                                                                                     #
#   Creating Hive Tables for all layers	         							          #
#                                                                                     #
#######################################################################################   
         
tables=($SUBJECT_AREA_HOME/*_tbl)
for table in "${tables[@]}"
do
    sh ${table}/bin/module.sh "setup" "${log_file_path}"
done

#######################################################################################
#                                                                                     #
#   Creating Postgres Sequence and Table			                                  #
#                                                                                     #
#######################################################################################   
         
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE_NAME \
   -a -f $PROJECT_HOME/rdbms_schema/postgres_schema.sql
exit_code=$?

success_message="Successfully created JOB_STATISTICS table in Postgres"
failure_message="Failed while creating JOB_STATISTICS table in Postgres"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${log_file_path}"

##############################################################################
#                                    End                                     #
##############################################################################