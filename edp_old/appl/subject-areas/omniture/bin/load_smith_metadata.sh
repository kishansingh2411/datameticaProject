#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: load_smith_metadata.sh 
#   Program type: Unix Bash Shell script
#   Purpose:    : This will load metadata into smith metadata tables                                                        
#   Author(s)   : DataMetica Team
#   Usage       : sh load_smith_metadata.sh $suite_name
#   Date        : 12/28/2015
#   Log File    : .../log/${suite_name}/${job_name}.log
#   SQL File    :                                 
#   Error File  : .../log/${suite_name}/${job_name}.log
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
#   Sourcing reference files  		                                                  #
#                                                                                     #
#######################################################################################   

source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/beeline.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/omniture.properties

#######################################################################################
#                                                                                     #
#   Local Param 				 			     				                      #
#                                                                                     #
#######################################################################################   

current_date=`date +"%Y-%m-%d"`
suite_name=$(echo "$1" | tr '[:upper:]' '[:lower:]')
start_time=`date +"%Y-%m-%d %H:%M:%S"`
log_file_dir="${LOG_DIR_SUBJECT_AREA}/${suite_name}"
log_file_path="${log_file_dir}/${job_name}.log"

suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')

if [ $suite_name == "$CHANNEL900" ]
then
   	job_name="STK36_600005_REFRSH_SMITH_META"
elif [ $suite_name == "cablevis-uow-com" ]
then
   	job_name="UOW_600002_REFRSH_SMITH_META"
else
	echo "Invalid suite_name provided"
	exit	
fi

##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"

##############################################################################
#																			 #
# Creating Local Log directory                                               #
#																			 #
##############################################################################

fn_local_create_directory_if_not_exists "${log_file_dir}" "${BOOLEAN_TRUE}" "${log_file_path}"

##############################################################################
#																			 #
# Initialization                                                             #
#																			 #
##############################################################################

export PGHOST=$POSTGRES_HOST;
export PGPORT=$POSTGRES_PORT;
export PGPASSWORD=$POSTGRES_PASSWORD;
batch_id=`psql -X -U $POSTGRES_USERNAME -P t -P format=unaligned $POSTGRES_DATABASE_NAME -c "select nextval('generate_seq_id')"`

if [ -z "$batch_id" ]
then
   fn_log_error "Failed while creating new BatchId!!!" "${log_file_path}"
   exit -1
else
   fn_log_info "Successfully created new BatchId [$batch_id]" "${log_file_path}"
fi

##############################################################################
#									   										 #
# Capture statistics                                                         #
#									  										 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}"

#######################################################################################
#                                                                                     #
#   Creating Metadata tables			                                              #
#                                                                                     #
#######################################################################################   

if [ $suite_name == "$CHANNEL900" ]
then
   tables=($SMITH_OPTIMUM_LINK_TBL \
        $SMITH_OPTIMUM_SUITE_TBL \
        $SMITH_PERIOD_TBL)
else
   tables=($SMITH_OPTIMUM_EVENT_LIST_TBL \
        $SMITH_OPTIMUM_LINK_TBL \
        $SMITH_OPTIMUM_SUITE_TBL \
        $SMITH_PERIOD_TBL)
fi

for table in "${tables[@]}"
do  
    table_name=`basename ${table}`
    
    sh $SUBJECT_AREA_HOME/${table}_tbl/bin/module.sh "prepare" "${log_file_path}"
    exit_code=$?
    
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to load smith metadata table ${table_name}, Quitting the process" "${log_file_path}"
	    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" ""
	fi
	
	fn_log_info "Successfully loaded smith metadata table ${table_name}" "${log_file_path}"  
			
done

fn_log_info "Load Metadata script completed!!!" "${log_file_path}"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${current_date}" "${log_file_path}" "NA" "$args"

#######################################################################################
#                                    End                                              #
#######################################################################################