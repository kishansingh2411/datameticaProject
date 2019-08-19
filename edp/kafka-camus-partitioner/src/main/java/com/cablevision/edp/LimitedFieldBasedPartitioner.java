package com.cablevision.edp;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.mapreduce.JobContext;

public class LimitedFieldBasedPartitioner extends FieldBasedPartitioner{
	
	@Override
	public String generatePartitionedPath(JobContext context, String topic, String encodedPartition) {
		String year = null, month = null; 
		List<String> topics = new ArrayList<String>(context.getConfiguration().getTrimmedStringCollection("kafka.whitelist.topics"));
		List<String> subDirs = new ArrayList<String>(context.getConfiguration().getTrimmedStringCollection("etl.destination.subdirs"));

		 try{
			Date loadDate = sdf.parse(encodedPartition);
			year = yearFormatter.format(loadDate);
			month = monthFormatter.format(loadDate);
		}catch(ParseException pe){
			logger.error(pe.getMessage());
		}
		 
		String partitiononPath = subDirs.get(topics.indexOf(topic)) + "/" + "load_year=" + year + "/load_month=" + month;
		logger.info("Partition Path = " + partitiononPath);
		return partitiononPath;
	}

}
