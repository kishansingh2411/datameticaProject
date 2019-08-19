package io.confluent.connect.hdfs.partitioner;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Schema.Type;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.hdfs.HdfsSinkConnectorConfig;
import io.confluent.connect.hdfs.errors.PartitionException;

public class CustomPartitioner implements Partitioner {
  private static final Logger log = LoggerFactory.getLogger(CustomPartitioner.class);
  private static String fieldName;
  private static String partitionLevel;
  private static List<String> destinationSubdirs;
  private static List<String> topics;
  private List<FieldSchema> partitionFields = new ArrayList<>();
  private Map<String, String> topicDirMap = new HashMap<String, String>();
  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
  SimpleDateFormat yearFormatter = new SimpleDateFormat("yyyy");
  SimpleDateFormat monthFormatter = new SimpleDateFormat("MM");
  SimpleDateFormat dateFormatter = new SimpleDateFormat("dd");

  @Override
  public void configure(Map<String, Object> config) {
    fieldName = (String) config.get(HdfsSinkConnectorConfig.PARTITION_FIELD_NAME_CONFIG);
    partitionLevel = (String) config.get(HdfsSinkConnectorConfig.TABLE_PARTITION_LEVEL);
    log.info("partition field is "+fieldName);
    destinationSubdirs = (List<String>) config.get(HdfsSinkConnectorConfig.DESTINATION_SUBDIRS_CONFIG);
    log.info("destination sub dir is "+destinationSubdirs);
    topics = (List<String>) config.get(HdfsSinkConnectorConfig.TOPICS_CONFIG);
    log.info("topics are "+topics);
    partitionFields.add(new FieldSchema("load_year", TypeInfoFactory.stringTypeInfo.toString(), ""));
    partitionFields.add(new FieldSchema("load_month", TypeInfoFactory.stringTypeInfo.toString(), ""));
    if(partitionLevel.equalsIgnoreCase("day")){
    	partitionFields.add(new FieldSchema("load_day", TypeInfoFactory.stringTypeInfo.toString(), ""));
    }

    for(int i=0,j=0; i<topics.size() && j<destinationSubdirs.size(); i++,j++){
    	topicDirMap.put(topics.get(i).trim(), destinationSubdirs.get(j).trim());
    }

    for (Map.Entry entry : topicDirMap.entrySet()) {
        System.out.println("Map values are ---->"+entry.getKey() + ", " + entry.getValue());
    }

  }

  @Override
  public String encodePartition(SinkRecord sinkRecord) {
    Object value = sinkRecord.value();
    Schema valueSchema = sinkRecord.valueSchema();
    if (value instanceof Struct) {
      Struct struct = (Struct) value;
      Object partitionKey = struct.get(fieldName);
      Type type = valueSchema.field(fieldName).schema().type();
      log.debug(" partitionKey : " + partitionKey + " type : "+ type + " value : "+ value + " valueSchema : "+ valueSchema + " struct : "+ struct);
      switch (type) {
        case INT8:
        case INT16:
        case INT32:
        case INT64:
        	Date key = (Date) partitionKey;
 			String year = yearFormatter.format(key);
 			String month = monthFormatter.format(key);
 			 if(partitionLevel.equalsIgnoreCase("day")){
 				 String date = dateFormatter.format(key);
 				 return new StringBuffer().append("load_year=").append(year).append("/load_month=").append(month).append("/load_day=").append(date).toString();
 			 }else{
 				return new StringBuffer().append("load_year=").append(year).append("/load_month=").append(month).toString();
 			 }
        case STRING:
          return fieldName + "=" + (String) partitionKey;
        case BOOLEAN:
          boolean booleanRecord = (boolean) partitionKey;
          return fieldName + "=" + Boolean.toString(booleanRecord);
        default:
          log.error("Type {} is not supported as a partition key.", type.getName());
          throw new PartitionException("Error encoding partition.");
      }
    } else {
      log.error("Value is not Struct type.");
      throw new PartitionException("Error encoding partition.");
    }
  }

  @Override
  public String generatePartitionedPath(String topic, String encodedPartition) {
	  log.info("encoded partition is "+encodedPartition);
	  String partitiononPath =  topicDirMap.get(topic) + "/" + encodedPartition;
	  log.info("Partition Path = " + partitiononPath);
	  return partitiononPath;
  }

  @Override
  public List<FieldSchema> partitionFields() {
    return partitionFields;
  }
}
