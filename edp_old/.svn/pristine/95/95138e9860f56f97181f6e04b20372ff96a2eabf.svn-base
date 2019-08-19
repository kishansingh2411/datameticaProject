/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.hdfs.avro;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.node.JsonNodeFactory;
import org.codehaus.jackson.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.hdfs.RecordWriter;
import io.confluent.connect.hdfs.RecordWriterProvider;

public class AvroRecordWriterProvider implements RecordWriterProvider {

  private static final Logger log = LoggerFactory.getLogger(AvroRecordWriterProvider.class);
  private final static String EXTENSION = ".avro";

  @Override
  public String getExtension() {
    return EXTENSION;
  }

  @Override
  public RecordWriter<SinkRecord> getRecordWriter(Configuration conf, final String fileName,
                                                        SinkRecord record, final AvroData avroData)
      throws IOException {
	log.info("Getting record writer");
    DatumWriter<Object> datumWriter = new GenericDatumWriter<>();
    final DataFileWriter<Object> writer = new DataFileWriter<>(datumWriter);
    Path path = new Path(fileName);

    final Schema schema = record.valueSchema();
    final FSDataOutputStream out = path.getFileSystem(conf).create(path);
    org.apache.avro.Schema avroSchema = avroData.fromConnectSchema(schema);
    log.info("record avro schema type "+avroSchema.getType());
    for (Field field: avroSchema.getFields())
    {
		log.debug("connect schema name "+schema.field(field.name()).schema().name());
		log.debug("connect schema parameters "+schema.field(field.name()).schema().parameters());
		log.debug("connect schema type "+schema.field(field.name()).schema().type());
		
		if (schema.field(field.name()).schema().name() != null && 
				schema.field(field.name()).schema().name().equals(Decimal.LOGICAL_NAME))
		{
			for (org.apache.avro.Schema type : field.schema().getTypes())
	    	{
				log.info("Got the following schema type "+type.getFullName() +" for field "+field.name());
				if (!type.getName().equals(org.apache.avro.Schema.Type.NULL.name()) && 
						type.getName().equalsIgnoreCase(org.apache.avro.Schema.Type.BYTES.name()))
				{
					int scale = 0;
					int precision = 38;
		    		log.info("avro schema type name "+type.getName());
		    		if (schema.field(field.name()).schema().parameters() != null && 
		    				schema.field(field.name()).schema().parameters().get(Decimal.SCALE_FIELD) != null)
		    		{
		    			scale = Integer.parseInt(schema.field(field.name()).schema().parameters().get(Decimal.SCALE_FIELD));
		    		}
		    		
	    			type.addProp("logicalType", "decimal");
	    			type.addProp("scale", JsonNodeFactory.instance.numberNode(scale));
	    			type.addProp("precision", JsonNodeFactory.instance.numberNode(precision));
				}
	    	}
		}

    }

    writer.create(avroSchema, out);

    return new RecordWriter<SinkRecord>(){
      @Override
      public void write(SinkRecord record) throws IOException {
        log.trace("Sink record: {}", record.toString());
        Object value = avroData.fromConnectData(schema, record.value());
        writer.append(value);
      }

      @Override
      public void close() throws IOException {
        writer.close();
      }
    };
  }
  
  private JsonNode parametersFromConnect(Map<String, String> params) {
	    ObjectNode result = JsonNodeFactory.instance.objectNode();
	    for (Map.Entry<String, String> entry : params.entrySet()) {
	      result.put(entry.getKey(), entry.getValue());
	    }
	    return result;
	  }
}
