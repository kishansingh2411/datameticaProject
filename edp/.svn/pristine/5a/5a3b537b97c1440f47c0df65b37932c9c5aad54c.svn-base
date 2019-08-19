#!/bin/sh

set -x
#user_name="${HADOOP_USER_NAME}"
user_name=`whoami`

conf_dir="/tmp/workdb"

log_dir="${conf_dir}/log"
log_file_name="${log_dir}/workdb_util_${user_name}"

if [ ! -e ${log_dir} ]
then
   mkdir -p ${log_dir}
   touch ${log_file_name}
fi

zookeeper_quorum="cvldhdpdn1.cscdev.com:2181,cvldhdpmn1.cscdev.com:2181,cvldhdpmn2.cscdev.com:2181"

hiveserver2_url="jdbc:hive2://${zookeeper_quorum}/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2"

###
# Checking mandatory parameters
#####
if [ ! $# == 6 ] && [ ! $# == 5 ]
then
   echo "One or more Mandatory parameters are missing." >> ${log_file_name}
   echo "Please provide the correct number of parameters." >> ${log_file_name}
   echo -e "Please provide correct number of parameters \n\t Minimum: 5\n\t\t1.Source File\n\t\t2.Table Name\n\t\t3.IsHeader present\n\t\t4.Is file of fixed width\n\t\t5.Delimiter of file \n\tMaximum: 6\n\t\tAll that mentioned above\n\t\t6.Is table needs to be shared(By default it is false)?" | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
fi

###
# Source file path that needs to be ingested 
#####
source_file_path="${1}"

###
# Table name
#####
table="${2}"

###
# Headers flag
#####
isHeaderPresent=`echo ${3} | tr '[:upper:]' '[:lower:]'`

###
# Deriving flag's value
#####
if [ -z ${isHeaderPresent} ] || [ "${isHeaderPresent}" == "yes" ] || [ "${isHeaderPresent}" == "y" ]
then
   isHeaderPresent="Y"
elif [ "${isHeaderPresent}" == "no" ] || [ "${isHeaderPresent}" == "n" ]
then
   isHeaderPresent="N"
fi

###
# Fixed Width flag
#####
fixedWidth=`echo ${4} | tr '[:upper:]' '[:lower:]'`

###
# Delimiter of source file
#####
delimiter=`echo -n ${5} | tail -c 2`

###
# Deriving fixed width flag's value
#####
if [ "${fixedWidth}" == "yes" ] || [ "${fixedWidth}" == "y" ]
then
   fixedWidth="Y"
elif [ -z ${fixedWidth} ] || [ "${fixedWidth}" == "no" ] || [ "${fixedWidth}" == "n" ]
then
   fixedWidth="N"
fi

###
# Shareable flag
#####
isShared=`echo ${6} | tr '[:upper:]' '[:lower:]'`

###
# Deriving shareable flag's value
#####
if [ ${isShared} == "yes" ] || [ ${isShared} == "y" ]
then
   isShared="Y"
elif [ -z ${isShared} ] || [ ${isShared} == "no" ] || [ ${isShared} == "n" ]
then
   isShared="N"
fi

data_layer_work="/edp/work/workdb"

###
# Validating Source file path,
#            Table name,
#            Delimiter
#####
if [ -z "${source_file_path}" ]
then
   echo "Source file path is missing, please provide valid path" >> ${log_file_name}
   echo "Source file path is missing, please provide valid path." | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
elif [ -z ${table} ]
then
   echo "Please provide table name" >> ${log_file_name}
   echo "Table name is missing, please provide table name." | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
elif [ -z ${delimiter} ] && [ "${fixedWidth}" == "N" ]
then
   echo "Delimiter of given source file is missing." >> ${log_file_name}
   echo "If Fixed width, please specify fixed width flag correctly." >> ${log_file_name}
   echo "Delimiter and fixedWidth flag can't be blank at same time, please provide correct delimiter or set fixedwidth flag." | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
fi

file_dir=`dirname "${source_file_path}"`
file_name_with_spaces=`basename "${source_file_path}"`

hadoop fs -test -e "${file_dir}"/"${file_name_with_spaces}"
exit_code=$?

if [ ! ${exit_code} == 0 ]
then
   echo "Source file does not exists, please provide correct source path" >> ${log_file_name}
   echo "Source file path does not exists." | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
fi

echo "Source file is present at ${source_file_path}" >> ${log_file_name}

number_of_spaces=`echo "${file_name_with_spaces}" | cut -d' ' -f1- | wc -w`

if [ ${number_of_spaces} -gt 1 ]
then
   file_name=$(echo ${file_name_with_spaces// /_})
   hadoop fs -mv "${file_dir}"/"${file_name_with_spaces}" "${file_dir}"/"${file_name}" &>> ${log_file_name}
else
   file_name="${file_name_with_spaces}"
fi

###
# Appending username if missing from table name
#####
if [[ ! ${table} == ${user_name}_* ]]
then
   table="${user_name}_${table}"
fi

db_name="workdb"
create_tbl_stmt="/tmp/${table}/tmp/create_tbl.hql"

if [ ! -e /tmp/${table}/tmp ]
then
   mkdir -p /tmp/${table}/tmp
fi

if [ -e ${create_tbl_stmt} ]
then
   rm -r ${create_tbl_stmt}
fi

touch ${create_tbl_stmt}
chmod 700 ${create_tbl_stmt}

###
# Extracting source file if it is binary
#####

file_ext=`echo "${file_name}" | cut -d'.' -f2-`

data_file=""
if [[ ${file_ext} == "zip" ]] || [[ ${file_ext} == "tar.gz" ]]
then
   hadoop ${conf_dir}/jar/utils-0.1-SNAPSHOT.jar com.cablevision.util.UntarUtil \
      "${file_dir}"/"${file_name}" "/tmp/${user_name}" &>> ${log_file_name}
   exit_code=$?

   if [ ${exit_code} -ne 0 ]
   then
      echo "Failed while extracting the file." >> ${log_file_name}
      echo "Failed while extracting the file refer attached logs for more details.\n\n\t${log_file_name}" | mail -s "Workdb util failed info" ${user_name}@cablevision.com
      mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log    
      exit -1
   fi
   echo "Extraction of source file is completed" >> ${log_file_name}

   extract_file_name=`echo "${file_name}" | cut -d'.' -f1`
   data_file=`hadoop fs -ls /tmp/${user_name}/"${extract_file_name}" | awk -F ' ' '{print \$8}' | tail -1`
else
   data_file="${file_dir}"/"${file_name}"
fi

hadoop fs -test -e /tmp/${user_name}/${table} &>> ${log_file_name}
exit_code=$?

if [ ${exit_code} == 0 ]
then
   hadoop fs -rm -r /tmp/${user_name}/${table} &>> ${log_file_name}
fi

hadoop fs -mkdir -p /tmp/${user_name}/${table} &>> ${log_file_name}
exit_code=$?

if [ ! ${exit_code} == 0 ]
then
   echo "Failed to delete the tmp directory" >> ${log_file_name}
   exit -1
fi

###
# Declaring an array of column headers
#####
declare -a column_headers

hadoop fs -test -e /tmp/${user_name}/${table}/columns &>> ${log_file_name}
exit_code=$?

if [ ${exit_code} == 0 ]
then
   hadoop fs -rm -r /tmp/${user_name}/${table}/columns &>> ${log_file_name}
fi

if [ "${isHeaderPresent}" == "Y" ]
then
   hadoop fs -cat "${data_file}" | head -1 | hadoop fs -put - /tmp/${user_name}/${table}/columns &>> ${log_file_name}
   column_headers=(`hadoop fs -cat /tmp/${user_name}/${table}/columns | tail | tr -s ' ' '_' | tr [=\${delimiter}=] ' '`)
elif [ "${fixedWidth}" == "Y" ]
then
   column_headers=(columns)
else
   column_count=`hadoop fs -cat "${data_file}" | awk -F [=${delimiter}=] '{print NF}' | sort -u | tail -1`
   for i in `eval echo {1..$column_count}`
   do
      index=$((i-1))
      column_headers[${index}]="COLUMN_${i}"
   done
fi

###
# Creating schema file for table to be invoked on hive.
#####
echo "USE ${db_name};" >> ${create_tbl_stmt}

if [ "${isShared}" == "Y" ]
then
   table="${table}_shared"
fi
echo "table :: ${table}"

echo "CREATE TABLE IF NOT EXISTS ${table}" >> ${create_tbl_stmt}
echo "(" >> ${create_tbl_stmt}

for columns in ${column_headers[@]}
do
   column=`echo ${columns} | tr -d '\r'`
   lst_ch=`echo -n $column | tail -c 1`
   frst_ch=`echo -n $column | head -c 1`

   if [ $lst_ch == '_' ]
   then
     column=`echo ${column} | rev | cut -c 2- | rev`
   fi

   if [ $frst_ch == '_' ]
   then
      column=`echo ${column} | cut -c 2-`
   fi

   if [[ "${columns}" == "${column_headers[${#column_headers[@]} - 1]}" ]]
   then
      echo "\`${column}\` STRING" >> ${create_tbl_stmt}
   else
      echo "\`${column}\` STRING," >> ${create_tbl_stmt}
   fi
done

echo ")" >> ${create_tbl_stmt}

if [ "${fixedWidth}" == "N" ]
then
   echo "ROW FORMAT DELIMITED" >> ${create_tbl_stmt}
   echo "FIELDS TERMINATED BY '${delimiter}'" >> ${create_tbl_stmt}
fi

echo "STORED AS TEXTFILE" >> ${create_tbl_stmt}
echo "LOCATION '${data_layer_work}/${table}'" >> ${create_tbl_stmt} 
echo "TBLPROPERTIES (" >> ${create_tbl_stmt}

if [ "${isHeaderPresent}" == "Y" ]
then
   echo "'skip.header.line.count'='1'," >> ${create_tbl_stmt}
fi

echo "'serialization.null.format'=''" >> ${create_tbl_stmt}
echo ")" >> ${create_tbl_stmt}
echo ";" >> ${create_tbl_stmt}

###
# Loading given file in created table
#####
echo "--Loading the data file in table" >> ${create_tbl_stmt}
echo "LOAD DATA" >> ${create_tbl_stmt}
echo "INPATH '${data_file}'" >> ${create_tbl_stmt}
echo "OVERWRITE INTO TABLE ${table}" >> ${create_tbl_stmt}
echo ";" >> ${create_tbl_stmt}

hadoop fs -test -e /tmp/${user_name}/workdb
exit_code=$?

if [ ${exit_code} == 0 ]
then
   hadoop fs -rm -r /tmp/${user_name}/workdb   
fi

hadoop fs -mkdir -p /tmp/${user_name}/workdb
#hadoop fs -copyFromLocal ${create_tbl_stmt} /tmp/${user_name}/workdb

hadoop fs -test -e /tmp/${user_name}/${table}/columns
exit_code=$?

if [ ${exit_code} == 0 ]
then
   hadoop fs -rm -r /tmp/${user_name}/${table}/columns
fi

###
# Creating and loading the Hive schema
#####
beeline -u "${hiveserver2_url}" -f ${create_tbl_stmt} >> ${log_file_name} 2>&1
exit_code=$?

if [ ! ${exit_code} == 0 ]
then
   echo "Failed while executing HQL using Hiveserver2" >> ${log_file_name}
   echo "Failed while executing HQL file, please refer attached logs.\n\n\t\t${log_file_name}" | mail -s "Workdb util failed info" ${user_name}@cablevision.com
   mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log
   exit -1
fi

###
# Setting the appropriate permissions at Table location
#####

if [ "${isShared}" == "Y" ]
then
   hadoop fs -chmod -R 755 ${data_layer_work}/${table}
else
   hadoop fs -chmod -R 700 ${data_layer_work}/${table}
fi

echo "Changed the permissions at table location as requested by user." >> ${log_file_name}

echo "Successfully created table ${table} in WORKDB." | mail -s "Workdb util success info" ${user_name}@cablevision.com

mv ${log_file_name} ${log_file_name}_`date +"%Y%m%d%H%M%S"`.log

###
# End
####
