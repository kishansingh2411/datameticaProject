package com.cablevision.edp;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;

import com.linkedin.camus.etl.IEtlKey;
import com.linkedin.camus.etl.Partitioner;

public class FieldBasedPartitioner extends Partitioner {

	protected static final Logger logger = Logger.getLogger(FieldBasedPartitioner.class);

	public static final String DEFAULT_TIME_ZONE = "America/Los_Angeles";
	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	SimpleDateFormat yearFormatter = new SimpleDateFormat("yyyy");
	SimpleDateFormat monthFormatter = new SimpleDateFormat("MM");
	SimpleDateFormat dateFormatter = new SimpleDateFormat("dd");

	@Override
	public String encodePartition(JobContext context, IEtlKey key) {
		String val = "No_value";

		logger.info("In method encodePartition....etlKey...."+key);
	    System.out.println("In method encodePartition....etlKey...."+key);
		Text textKey = new Text("load_date");
		if (key.getPartitionMap().containsKey(textKey)) {
			logger.info("In method encodePartition....map contain key");
			System.out.println("In method encodePartition....map contain key");
			val = ((Text) key.getPartitionMap().get(textKey)).toString();
		}
		logger.info("In method encodePartition....Getting Load date----"+val);
		System.out.println("In method encodePartition....Getting Load date----"+val);
		return val;
	}

	@Override
	public String generatePartitionedPath(JobContext context, String topic, String encodedPartition) {
		String year = null, month = null, date = null;
		List<String> topics = new ArrayList<String>(context.getConfiguration().getTrimmedStringCollection("kafka.whitelist.topics"));
		List<String> subDirs = new ArrayList<String>(context.getConfiguration().getTrimmedStringCollection("etl.destination.subdirs"));
		
		 try{
			Date loadDate = sdf.parse(encodedPartition);
			year = yearFormatter.format(loadDate);
			month = monthFormatter.format(loadDate);
			date = dateFormatter.format(loadDate);
		}catch(ParseException pe){
			logger.error(pe.getMessage());
		}
		 
		String partitiononPath =  subDirs.get(topics.indexOf(topic))+ "/" + "load_year=" + year + "/load_month=" + month + "/load_day=" + date;
		logger.info("Partition Path = " + partitiononPath);
		return partitiononPath;
	}

	@Override
	public String generateFileName(JobContext context, String topic, String brokerId, int partitionId, int count, long offset, String encodedPartition) {
		return topic + "." + brokerId + "." + partitionId + "." + count + "." + offset + "." + encodedPartition + "." + System.currentTimeMillis();
	}

	@Override
	public String getWorkingFileName(JobContext context, String topic, String brokerId, int partitionId, String encodedPartition) {
		return "data." + topic.replace('.', '_') + "." + brokerId + "." + partitionId + "." + encodedPartition;
	}
}
