#!/bin/sh

######################################################################################################################
#   This source code is the property of:
#   Cablevision Systems Corporation, Inc. (c) 2015
#   1111 Stewart Avenue, Bethpage, NY 11714
#   www.cablevision.com
#   Department: AM
#
#   Program name: validate_move_to_incoming.sh  
#   Program type: Unix Bash Shell script
#   Purpose:    : This will validate extracted files for following condition           
#                   -  File size is greater than zero                                 
#                   -  Validate column header                                         
#                 If validation is successful, source data file will move respective   
#                 incoming directory 
#                 Input Arguments for this script is: key_Param_id.                                                       
#   Author(s)   : DataMetica Team
#   Usage       : sh validate_move_to_incoming.sh  key_Param_id
#   Date        : 09/12/2016
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

SUBJECT_AREA_HOME=`dirname ${SCRIPT_HOME}`
SUBJECT_AREAS_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREAS_HOME}`

###                                                                           
# Load all environment properties files
source $PROJECT_HOME/etc/namespace.properties
source $PROJECT_HOME/etc/default.env.properties
source $PROJECT_HOME/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $SUBJECT_AREA_HOME/etc/optimum_app.properties
source $PROJECT_HOME/bin/functions.sh
source $SUBJECT_AREA_HOME/bin/omniture_functions.sh

###############################################################################
#                                                                             #
# Checking number of parameter passed                                         #
#                                                                             #
###############################################################################

if [ "$#" -ne 1 ]
then
  echo "Illegal number of parameters.  Parameters expected [key_param_id]"
  exit 
fi

##############################################################################
#																			 #
# Local Params				 								 	 			 #
#																			 #
##############################################################################

key_param_id="$1"
suite_data_file_path=$SUBJECT_AREA_HOME/metadata/Optimum_Suite.txt

params="$(fn_get_omniture_params ${key_param_id})"

if [ -z "$params" ]
then
    echo "Invalid Key-Param Id found : $key_param_id"
    exit
fi

source_date=$(echo $params | cut -f 1 -d '~'| cut -f 1 -d ' ')
suite_name=$(echo $params | cut -f 2 -d '~')
suite_name=$(echo "$suite_name" | tr '[:upper:]' '[:lower:]')

job_name="$(fn_generate_job_name "$key_param_id" "$suite_name" "VDATE_MV_TO_INCOM")"

start_time=`date +"%Y-%m-%d %H:%M:%S"`
current_batch_id_path="${BATCH_ID_DIR}/$suite_name/current"
batch_id=`cat $current_batch_id_path`
log_file_path="${LOG_DIR_SUBJECT_AREA}/${suite_name}/${job_name}.log"

##############################################################################
#																			 #
# Checking mandatory Params                                                  #
#																			 #
##############################################################################

fn_check_mandatory_params "suite_name,$suite_name"
  
##############################################################################
#                                                                            #
# Checking Correct Suite Name                                                #
#                                                                            #
##############################################################################

fn_check_suite_name "$suite_name"
  
##############################################################################
#																			 #
# Checking date format		                                                 #
#																			 #
##############################################################################

fn_check_date_format "$source_date"

##############################################################################
#																			 #
# Capture statistics                                                         #
#																			 #
##############################################################################

fn_insert_job_statistics "${batch_id}" "${job_name}" "${start_time}" "RUNNING" "${log_file_path}" 

##############################################################################
#                                                                            #
# Get suite id                                                               #
#                                                                            #
##############################################################################

suite_name=$(echo $suite_name | tr '[:upper:]' '[:lower:]')
source_file_dir="$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING/$suite_name"

source_file_path=`hadoop fs -ls \
                  ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/$suite_name/*$source_date*.gz | \
                  awk '{print$8}'`
                  
source_file_name=`basename $source_file_path`

while read -r line
do
   suite_name_file=$(echo $line | cut -f 2 -d ',')
   suite_name_file=$(echo $suite_name_file | tr '[:upper:]' '[:lower:]')
   if [ "$suite_name_file" == "$suite_name" ]
   then
      suite_id=$(echo $line | cut -f 1 -d ',')
	  omniture_file_name=$(echo $line | cut -f 4 -d ',')
	  break
   fi
done< $suite_data_file_path

##############################################################################
#                                                                            #
# Validating each extracted file for following                               #
#     - Column header match for data hit file                                #  
#     - File size greater than zero                                          #
#                                                                            #
##############################################################################

fn_validate_column_header "${suite_name}" \
   "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/${suite_name}/${omniture_file_name}_${source_date}" \
   "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LOOKUP}" "${source_date}" "${log_file_path}"
exit_code=$?

if [ $exit_code == $EXIT_CODE_FAIL ]; 
then
   fn_log_error "For suite $suite_name, column header validation failed, Quitting the Process" "${log_file_path}"
   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" \
      "${source_date}" "${log_file_path}" "${source_file_name}" " "
else
   array_tsv_files=`hadoop fs -ls \
       ${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/${suite_name}/${omniture_file_name}_${source_date}/*.tsv | \
       awk '{print$8}'`
   for tsv_file in ${array_tsv_files[@]}
  do
     actual_size=`hadoop fs -du $tsv_file | cut -d' ' -f1`
	  tsv_fileName="$(basename $tsv_file | cut -d"." -f1)"	  
	  
	  fn_log_info "For suite $suite_name, file $tsv_fileName has size $actual_size" "${log_file_path}"
	     
	  if [ $actual_size -gt 0 ] 
	    then
	     if [[ $tsv_file == *"browser_type"* ]] || [[ $tsv_file == *"color_depth"* ]] || \
	        [[ $tsv_file == *"column_headers"* ]] || [[ $tsv_file == *"resolution"* ]] || \
	        [[ $tsv_file == *"event"* ]] || [[ $tsv_file == *"operating_system_type"* ]] || \
	        [[ $tsv_file == *"referrer"* ]]
	    then
          #continue because we need to ignore this files            
	        continue
	   elif [[ $tsv_file == *"hit_data"* ]]
	     then	       
	                     
	      source_file_path=$tsv_file
	      destination_file_path="$DATA_LAYER_DIR_WORK_SUBJECT_AREA_DATA/work_hit_data_opt_app/suite_name=${suite_name}/source_date=${source_date}/"
	      pig_file_path=$SUBJECT_AREA_HOME/common/pig/remove_carriage.pig
	      script_path=${DATA_LAYER_DIR_CACHE_SUBJECT_AREA}/remove_carriage.ksh
	                      
	      fn_hadoop_delete_directory_if_exists "$destination_file_path" \
	      "${BOOLEAN_FALSE}" "${log_file_path}"
	                    
	      exit_code=$?   
	    
	    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	     then
	    	fn_log_error "Failed to create directory at work layer, Quitting the prossess..!!!" "${log_file_path}"
	  	    fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	    fi     
	                      
	     pig -p source_file_path="${source_file_path}" \
	     -p destination_file_path="${destination_file_path}" \
	     -p log_file_path="${log_file_path}" \
	     -p hdfs_script_path="$script_path" \
	     "$pig_file_path" 1>> "${log_file_path}" 2>> "${log_file_path}"
	     exit_code=$? 

	    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	     then
	       fn_log_error "For suite $suite_name, failed to move hit data file into work hit data table, Quitting the prossess..!!!" "${log_file_path}"
	  	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	    fi   
                          
	    else
	     fn_hadoop_move_file_or_directory "$tsv_file" \
	      "$DATA_LAYER_DIR_WORK_SUBJECT_AREA_LOOKUP/work_$tsv_fileName/suite_id=${suite_id}/source_date=${source_date}/" \
	      "${BOOLEAN_FALSE}" "${BOOLEAN_FALSE}" "${log_file_path}"
	       exit_code=$?    
    
           if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    	    then
    		   fn_log_error "For suite $suite_name, failed to move lookup file $tsv_fileName into work table work_$tsv_fileName, Quitting the prossess..!!!" "${log_file_path}"
  	     	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " " 
	        fi
        fi
	      else
	  	   fn_log_error "For suite $suite_name, file $tsv_file has size zero, Quitting the process and moving ${omniture_file_name}_${source_date}.tar.gz into bad_data directory" "${log_file_path}"
	       fn_hadoop_move_file_or_directory \
	        "${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_LANDING}/${suite_name}/${omniture_file_name}_${source_date}".tar.gz \
	        "$DATA_LAYER_DIR_INCOMING_SUBJECT_AREA_BADDATA/source_date=${source_date}" \
	        "${BOOLEAN_TRUE}" "${BOOLEAN_FALSE}" "${log_file_path}"
	       exit_code=$?    
    
     if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    	then
    	  fn_log_error "For suite $suite_name, failed to move source data file into bad data directory, Quitting the prossess..!!!" "${log_file_path}"
  	      fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " " 
	 fi
	            
	   fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${EXIT_CODE_FAIL}" "${source_date}" "${log_file_path}" "${source_file_name}" " "
	fi
  done
fi

##############################################################################
#                                                                            #
# Capture statistics                                                         #
#                                                                            #
##############################################################################

fn_update_job_statistics "${batch_id}" "${job_name}" "${start_time}" "NA" "${exit_code}" "${source_date}" "${log_file_path}" "${source_file_name}" " "

##############################################################################
#                                    End                                     #
##############################################################################