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
#                 1. SetUp mrdvr Environment											  
#  		          2. Create Hive tables                                						  
#                 3. Create postgres tables  
#   Author(s)   : DataMetica Team
#   Usage       : sh deployment.sh
#   Date        : 05/05/2016
#   Log File    : .../log/channel_tunnning/CHANNEL_TUNING_DEPLOYMENT.log
#   SQL File    : 
#   Error File  : .../log/channel_tunnning/CHANNEL_TUNING_DEPLOYMENT.log
#   Dependency  : 
#   Disclaimer  : 
#
#   Modification History :
#   =======================
#
#    ver     who                      date             comments
#   ------   --------------------     ----------       -------------------------------------
#    1.0     DataMetica Team          05/05/2016       Initial version
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

chmod 700 $PROJECT_HOME/etc/beeline.properties
chmod 700 $PROJECT_HOME/etc/postgres.properties

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
source $SUBJECT_AREA_HOME/etc/channel_tuning.properties

#######################################################################################
#                                                                                     #
#   Local Param 				 			     				                      #
#                                                                                     #
#######################################################################################   

log_file_path=${LOG_DIR}/${SUBJECT_AREA_NAME}/CHANNEL_TUNING_DEPLOYMENT.log

#######################################################################################
#                                                                                     #
#   Creating Local directories 				 			     			              #
#                                                                                     #
#######################################################################################   

local_dirs=("${LOG_DIR}/${SUBJECT_AREA_NAME}" \
            "${BATCH_ID_DIR_SUBJECT_AREA}")
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

fn_log_info "Local Directory created successfully" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating HDFS directories for all data layer 				 			          #
#                                                                                     #
#######################################################################################   

hdfs_dirs=("${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_DATA}" \
		   "${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
           "${DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA}" \
           "${DATA_LAYER_DIR_SMITH_SUBJECT_AREA}")
for hdfs_dir in "${hdfs_dirs[@]}"
do
   fn_hadoop_create_directory_if_not_exists ${hdfs_dir} ${BOOLEAN_TRUE} ${log_file_path}
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to create HDFS directory ${hdfs_dir}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
done

fn_log_info "HDFS Directory created successfully" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating Hive databases for all layers	         					              #
#                                                                                     #
#######################################################################################   

hive_databases=("${HIVE_DATABASE_NAME_INCOMING}" \
			    "${HIVE_DATABASE_NAME_GOLD}" \
                 "${HIVE_DATABASE_NAME_WORK}" \
                 "${HIVE_DATABASE_NAME_SMITH}")
for hive_database in ${hive_databases[@]}
do
   date_layer_dir=''
   case ${hive_database} in
       ${HIVE_DATABASE_NAME_INCOMING})
	   date_layer_dir="${DATA_LAYER_DIR_INCOMING}/${SUBJECT_AREA_NAME}"
	   ;;
	   ${HIVE_DATABASE_NAME_GOLD})
	   date_layer_dir="${DATA_LAYER_DIR_GOLD}/${SUBJECT_AREA_NAME}"
	   ;;
	   ${HIVE_DATABASE_NAME_WORK})
       date_layer_dir="${DATA_LAYER_DIR_WORK}/${SUBJECT_AREA_NAME}"
       ;;
       ${HIVE_DATABASE_NAME_SMITH})
       date_layer_dir="${DATA_LAYER_DIR_SMITH}/${SUBJECT_AREA_NAME}"
       ;;
    esac
   fn_create_hive_database "${hive_database}" "${date_layer_dir}" "${HIVESERVER2_URL}" "${HIVE_USENAME}" "${HIVE_PASSWORD}" "${log_file_path}"
done

fn_log_info "Hive Databases created successfully" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating Hive Tables for all layers	         							          #
#                                                                                     #
#######################################################################################   
tables=("gold_nz_t_f_split_channel_tuning_6mth_tbl" \
		"gold_nz_t_f_split_channel_tuning_curr_tbl" \
		"gold_nz_t_f_split_channel_tuning_rst_tbl" \
		"gold_t_cvb_f_split_dvr_usage_tbl" \
		"gold_t_cvb_f_split_mrdvr_usage_tbl" \
		"gold_t_f_split_ipchannel_tuning_tbl" \
		"incoming_nz_t_f_split_channel_tuning_6mth_tbl" \
		"incoming_nz_t_f_split_channel_tuning_curr_tbl" \
		"incoming_t_cvb_f_split_dvr_usage_tbl" \
		"incoming_t_cvb_f_split_mrdvr_usage_tbl" \
		"incoming_t_d_channel_name_tbl" \
		"incoming_t_d_household_attribute_tbl" \
		"incoming_t_d_household_device_tbl" \
		"incoming_t_d_period_tbl" \
		"incoming_t_d_res_demographic_tbl" \
		"incoming_t_d_stb_collection_task_tbl" \
		"incoming_t_d_tms_program_tbl" \
		"incoming_t_f_split_ipchannel_tuning_tbl" \
		"smith_t_d_channel_name_tbl" \
		"smith_t_d_household_attribute_tbl" \
		"smith_t_d_household_device_tbl" \
		"smith_t_d_period_tbl" \
		"smith_t_d_res_demographic_tbl" \
		"smith_t_d_stb_collection_task_tbl" \
		"smith_t_d_tms_program_tbl")         
for table in "${tables[@]}"
do
    sh $SUBJECT_AREA_HOME/${table}/bin/module.sh "setup" "${log_file_path}"
done

fn_log_info "Hive Tables created successfully" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating Postgres Sequence and Table			                                  #
#                                                                                     #
#######################################################################################   
         
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -h $POSTGRES_HOST -p $POSTGRES_PORT -U $POSTGRES_USERNAME -d $POSTGRES_DATABASE_NAME \
   -a -f $PROJECT_HOME/rdbms_schema/postgres_schema.sql
exit_code=$?

success_message="Successfully created schema in Postgres"
failure_message="Failed while creating schema in Postgres"
fn_handle_exit_code "${exit_code}" "${success_message}" "${failure_message}" "${BOOLEAN_TRUE}" "${log_file_path}"

#######################################################################################
#                                    End                                     		  #
#######################################################################################