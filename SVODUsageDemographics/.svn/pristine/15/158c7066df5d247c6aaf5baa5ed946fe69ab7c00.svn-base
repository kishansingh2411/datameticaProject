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
#   Usage       : sh deployment.sh 
#   Date        : 03/18/2017
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
#    1.0     DataMetica Team          03/18/2017      Initial version
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
#find $SUBJECT_AREAS_HOME -type f -exec dos2unix {} \;
chmod 755 -R $PROJECT_HOME
find $SUBJECT_AREAS_HOME -type f ! \( -name '*.jar' \) -exec dos2unix {} \;
find $PROJECT_HOME/common/bin/functions.sh -type f -exec dos2unix {} \;
find $PROJECT_HOME/common/etc/* -type f -exec dos2unix {} \;

#######################################################################################
#                                                                                     #
#   Sourcing reference files  		                                                  #
#                                                                                     #
#######################################################################################   

source $PROJECT_HOME/common/etc/namespace.properties
source $PROJECT_HOME/common/etc/beeline.properties
source $PROJECT_HOME/common/etc/default.env.properties
source $PROJECT_HOME/common/etc/postgres.properties
source $SUBJECT_AREA_HOME/etc/subject-area.env.properties
source $PROJECT_HOME/common/bin/functions.sh
source $SUBJECT_AREA_HOME/etc/svodusagedemographics.properties

#######################################################################################
#                                                                                     #
#   Local Param 				 			     				                      #
#                                                                                     #
#######################################################################################   

current_timestamp=`date +%Y%m%d%H%M%S`
log_file_path=$LOG_DIR_SUBJECT_AREA/SVODUSAGEDEMOGRAPHICS_DEPLOYMENT_${current_timestamp}.log

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
   fn_log_info "Successfully created local directory ${local_dir}" "${log_file_path}"
done

#######################################################################################
#                                                                                     #
#   Creating HDFS directories for all data layer 				 			          #
#                                                                                     #
#######################################################################################   

hdfs_dirs=("${DATA_LAYER_DIR_GOLD_SUBJECT_AREA}" \
			"${DATA_LAYER_DIR_INCOMING_SUBJECT_AREA}" \
			"${HDFS_APP_PREFIX_SUBJECT_AREA_OOZIE_LIB}" \
			"${HDFS_APP_PREFIX_SUBJECT_AREA_ETC}")
        
for hdfs_dir in "${hdfs_dirs[@]}"
do
   fn_hadoop_create_directory_if_not_exists "${hdfs_dir}" "${BOOLEAN_FALSE}" "${log_file_path}"
   exit_code=$?
   
   if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to create HDFS directory ${hdfs_dir}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
	fn_log_info "Successfully created HDFS directory ${hdfs_dir}" "${log_file_path}"
done

#######################################################################################
#																				      #
#   copying the oozie workflow xml files											  #
#																					  #
#######################################################################################

# hdfs dfs -copyFromLocal /usr/hdp/current/hive-server2/conf/hive-site.xml /app/util/SvodUsageDemographics/conf

files=("workflow.xml" \
	   "coordinator.xml" \
	   "svodusagedemographics-schema.properties")
	   
for file in "${files[@]}"
do
     if [[ ${file} == "workflow.xml" ]] 
	  then  
		source_file_path="${SUBJECT_AREA_HOME}/oozie/workflow.xml"
		target_directory="${HDFS_APP_PREFIX_SUBJECT_AREA_OOZIE}"
	  elif [[ ${file} == "coordinator.xml" ]] 
	  then
	    source_file_path="${SUBJECT_AREA_HOME}/oozie/coordinator.xml"
		target_directory="${HDFS_APP_PREFIX_SUBJECT_AREA_OOZIE}"
	 elif [[ ${file} == "svodusagedemographics-schema.properties" ]]
	 then
	   source_file_path="${SUBJECT_AREA_HOME}/etc/svodusagedemographics-schema.properties"
		target_directory="${HDFS_APP_PREFIX_SUBJECT_AREA_ETC}"
	 fi
	  
	 fn_copy_from_local \
      "${source_file_path}" \
      "${target_directory}" \
      "${BOOLEAN_FALSE}" \
      "${BOOLEAN_FALSE}" \
      "${log_file_path}"
   	  exit_code=$?
      if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
      then
   	      fn_log_error "Failed to copy file ${file} to HDFS directory ${target_directory}" "${log_file_path}"	
          return $exit_code
      fi
      fn_log_info "Successfully copied file ${file} to HDFS directory ${target_directory}" "${log_file_path}"
done	

#######################################################################################
#                                                                                     #
#   Copy Jar files to HDFS location     	         					              #
#                                                                                     #
#######################################################################################

source_file_path=${SUBJECT_AREA_HOME}/lib/SVODUsageDemographics-1.0-SNAPSHOT.jar
target_directory=${HDFS_APP_PREFIX_SUBJECT_AREA_OOZIE_LIB}

    fn_copy_from_local \
      "${source_file_path}" \
      "${target_directory}" \
      "${BOOLEAN_FALSE}" \
      "${BOOLEAN_FALSE}" \
      "${log_file_path}"
   	exit_code=$?
    if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
    then
        fn_log_error "Failed to copy file jar files to HDFS directory ${target_directory}" "${log_file_path}"	
        exit $exit_code
    fi
    fn_log_info "Successfully copied file Jar files to HDFS directory ${target_directory}" "${log_file_path}"
 	
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
for table in "${tables[@]}"; do  
    table_name=`basename ${table}`
    table_layer=`echo $table_name | cut -d'_' -f1`
    sh $table/bin/module.sh "setup" "${log_file_path}"
    exit_code=$?
    
	if [ ${exit_code} -ne $EXIT_CODE_SUCCESS ]
	then
	    fn_log_error "Failed to create table ${table_name}, Quitting the process"  "${log_file_path}"
		exit -1
	fi
	fn_log_info "Successfully created table ${table_name}" "${log_file_path}"

done


#######################################################################################
#                                    End                                              #
#######################################################################################