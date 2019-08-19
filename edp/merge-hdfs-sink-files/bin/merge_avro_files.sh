#!/bin/bash

hdfs_location=`beeline -u "jdbc:hive2://cvldhdpdn1.cscdev.com:2181,cvldhdpmn1.cscdev.com:2181,cvldhdpmn2.cscdev.com:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2" -n etlmgr -p "welcome1" --outputformat=tsv2 -e "describe formatted $1.$2" | grep Location | awk '{print$2}'`

hdfs dfs -test -e $hdfs_location
result=$?
if [ $result -ne 0 ]
then
	echo "Could not fetch the hdfs location of table $1.$2, exiting...."
	exit 1;
fi

echo "hdfs base path $hdfs_location"

file_array=`hdfs dfs -ls -R $hdfs_location | grep .avro | awk '{print $8}'`
dir_path=()
for i in ${file_array[@]}
do
	dir_name=`dirname $i`
	dir_path=(${dir_path[@]} $dir_name)
	echo "directory path $dir_path"
done

unique_dir_paths=`printf "%s\n" "${dir_path[@]}" | sort -u`

echo "unique directory paths $unique_dir_paths"

for hdfs_partition_directory_path in $unique_dir_paths
do
        echo "processing partition $hdfs_partition_directory_path"
        file_count=`hdfs dfs -ls $hdfs_partition_directory_path/*+*+*+*.avro | wc -l`
        if [ $file_count -ge 3 ]
        then
		hdfs dfs -mkdir -p $hdfs_partition_directory_path/temp
		hdfs dfs -rm $hdfs_partition_directory_path/temp/*
		hdfs dfs -mv $hdfs_partition_directory_path/*.avro $hdfs_partition_directory_path/temp

		for file_path in `hdfs dfs -ls $hdfs_partition_directory_path/temp | awk '{print $8}'`
		do
				file_name=`basename $file_path`
				echo "file name $file_name"
				if [ -z $start_offset ]
				then
					start_offset=`echo $file_name | cut -d"+" -f3`
					file_name_prefix=`echo $file_name | cut -d"+" -f1`+`echo $file_name | cut -d"+" -f2`+
				fi

				end_offset=`echo $file_name | cut -d"+" -f4 | cut -d"." -f1`
		done

		echo "start offset is $start_offset, end offset is $end_offset"
		echo "file name prefix is $file_name_prefix"
		local_tmp_path=/tmp/$1_$2
		mkdir -p $local_tmp_path
		rm $local_tmp_path/*
		hdfs dfs -get $hdfs_partition_directory_path/temp/* $local_tmp_path
		java -jar /home/etlmgr/avro-tools-1.7.7.jar concat $local_tmp_path/* $local_tmp_path/$file_name_prefix$start_offset+$end_offset.avro
		if [ $? -ne 0 ]
                then
                       echo "Failed while  merging the files"
                       hdfs dfs -mv $hdfs_partition_directory_path/temp/* $hdfs_partition_directory_path
                else

			hdfs dfs -mkdir -p $hdfs_partition_directory_path/_tmp
			hdfs dfs -rm $hdfs_partition_directory_path/_tmp/*
			hdfs dfs -put $local_tmp_path/$file_name_prefix$start_offset+$end_offset.avro $hdfs_partition_directory_path/_tmp
			hdfs dfs -mv $hdfs_partition_directory_path/_tmp/$file_name_prefix$start_offset+$end_offset.avro $hdfs_partition_directory_path/$file_name_prefix$start_offset+$end_offset.avro
	        	if [ $? -ne 0 ] 
			then
	               		echo "Failed to move the merged file"
	               		hdfs dfs -mv $hdfs_partition_directory_path/temp/* $hdfs_partition_directory_path
	        	fi
		fi

		hdfs dfs -rm -R $hdfs_partition_directory_path/temp
		hdfs dfs -rm -R $hdfs_partition_directory_path/_tmp
		rm $local_tmp_path/*
	else
		echo "skipping partition $hdfs_partition_directory_path"
        fi
done

