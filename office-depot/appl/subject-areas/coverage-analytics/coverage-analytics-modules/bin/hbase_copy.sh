   #!/bin/bash

#######################################################################################
#                              General Details                                        #
#######################################################################################
#                                                                                     #
# Name                                                                                #
#     : hbase_copy                                                                    #
# File                                                                                #
#     : hbase_copy.sh                                                                 #
# Description                                                                         #
#     : Loads data into hbase via phoenix                                             #
#                                                                                     #
#                                                                                     #
#                                                                                     #
# Author                                                                              #
#     : Shweta                                                                        #
#                                                                                     #
#######################################################################################

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
MODULE_HOME=`dirname ${SCRIPT_HOME}`
MODULES_HOME=`dirname ${MODULE_HOME}`
SUBJECT_AREA_HOME=`dirname ${MODULES_HOME}`
PROJECT_HOME=`dirname ${SUBJECT_AREA_HOME}`
PROJECTS_HOME=`dirname ${PROJECT_HOME}`

source $MODULE_HOME/bin/coverageAnalytics_functions.sh
source $MODULE_HOME/etc/deployment.properties
##############################################################################
#                                                                                                                                                        #
# Local Params                                                                                                                           #
#                                                                                                                                                        #
##############################################################################

startTime=`date +"%T"`
batchId=`cat $SUBJECT_AREA_HOME$BATCH_ID_FILE`
jobName=$HBASE_SCHEMA_SSH_ACTION
logFileName=$jobName"_"$batchId".log"
logFilePath=$PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName

##############################################################################
#                                                                                                                                                        #
# Reading current date                                                                                                       #
#                                                                                                                                                        #
##############################################################################

cd $HOME

if [ "$(ls -A $PROJECTS_HOME$OUTGOING/$1/$batchId/)" ]
then
   rm -f $PROJECTS_HOME$OUTGOING/$1/$batchId/*.*
fi

fn_log_info "Deleting file from outgoing directory [$PROJECTS_HOME$OUTGOING/$1/$batchId/*.*]" "${logFilePath}"

#Copying files from cluster to local in outgoing directory.
hadoop fs -copyToLocal $OUTPUT_DIRECTORY/$1/$batchId/$MERGED_OUTPUT/part-* $PROJECTS_HOME$OUTGOING/$1/$batchId/

ls $PROJECTS_HOME$OUTGOING/$1/$batchId/ > files.txt

for i in `cat files.txt`
   do
      #Logic for renaming part file to part.csv file.
      filename=$i
      ext=".csv"
      ren_filename=$filename
      ren_filename+=$ext
      arg=$PROJECTS_HOME$OUTGOING/$1/$ren_filename
      mv $PROJECTS_HOME$OUTGOING/$1/$batchId/$i $PROJECTS_HOME$OUTGOING/$1/$batchId/$ren_filename
      sed 's/\"//g' $ren_filename >> $ren_filename 
   done

## copying files from  cluster to local ####

ls $PROJECTS_HOME$OUTGOING/$1/$batchId > files.txt

for k in `cat files.txt`
  do
      arg=$PROJECTS_HOME$OUTGOING/$1/$batchId/$k
      hbase_table_name=$HBASE_TABLE$1
      hbase_table_name=$(echo $hbase_table_name | tr 'a-z' 'A-Z')
      logFileName=$HBASE_SCHEMA_SSH_ACTION"_"$batchId".log"

      #####Upserting data into Phoenix.   ##

      cd $PHOENIX_HOME

      ./psql.py -t $hbase_table_name --d $'\t' $hbase_ip:$hbase_port $arg > $PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName
      recordCount=`cat $PROJECTS_HOME$PROJ_TMP/$batchId/$logFileName | grep "CSV Upsert complete*" | cut -d " " -f4 `
      fn_log_info "HBase table [$hbase_table_name] Upserted with [$recordCount] records" "${logFilePath}"
  done

exitCode="$?"
endTime=`date +"%T"`
fn_job_statistics "$startTime" "$endTime" "$exitCode" "$1" "$recordCount" "$jobName" "$NOTAPPLICABLE" "$logFileName"

##############################################################################
#                                    End                                     #
##############################################################################

     