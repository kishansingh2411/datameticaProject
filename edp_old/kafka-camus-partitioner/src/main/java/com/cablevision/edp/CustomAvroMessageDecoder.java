package com.cablevision.edp;

import com.linkedin.camus.coders.CamusWrapper;
import com.linkedin.camus.coders.MessageDecoder;
import com.linkedin.camus.coders.MessageDecoderException;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Properties;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;

public class CustomAvroMessageDecoder extends MessageDecoder<byte[], Record> {
	
	private static final Logger logger = Logger.getLogger(CustomAvroMessageDecoder.class);
	
	private static final byte MAGIC_BYTE = 0x0;
	private static final int idSize = 4;
	private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
	private static final String MAX_SCHEMAS_PER_SUBJECT = "max.schemas.per.subject";
	private static final String DEFAULT_MAX_SCHEMAS_PER_SUBJECT = "1000";
	private static final String IS_NEW_PRODUCER = "is.new.producer";
	private static final String PARTITION_FIELD_NAME = "etl.partitioner.fieldName";
	protected DecoderFactory decoderFactory;
	private SchemaRegistryClient schemaRegistry;
	private Schema latestSchema;
	private int latestVersion;
	private String topic;
	private boolean isNew;
	private static String partitionFieldName;
	private static final String DEFAULT_PARTITION_FIELD_NAME = "LOAD_DATE";

	public CustomAvroMessageDecoder(SchemaRegistryClient schemaRegistry) {
		this.schemaRegistry = schemaRegistry;
	}

	public CustomAvroMessageDecoder() {
	}

	@Override
	public void init(Properties props, String topicName) {
		super.init(props, topicName);

		decoderFactory = DecoderFactory.get();
		if (props == null) {
			throw new IllegalArgumentException("Missing schema registry url!");
		}
		String baseUrl = props.getProperty(SCHEMA_REGISTRY_URL);
		if (baseUrl == null) {
			throw new IllegalArgumentException("Missing schema registry url!");
		}
		String maxSchemaObject = props.getProperty(MAX_SCHEMAS_PER_SUBJECT, DEFAULT_MAX_SCHEMAS_PER_SUBJECT);
		if (schemaRegistry == null) {
			schemaRegistry = new CachedSchemaRegistryClient(baseUrl, Integer.parseInt(maxSchemaObject));
		}
		this.isNew = Boolean.parseBoolean(props.getProperty(IS_NEW_PRODUCER, "true"));
		this.topic = topicName;
		partitionFieldName = props.getProperty(PARTITION_FIELD_NAME);
		if(partitionFieldName == null){
			logger.info("Could not get the partition field name from Properties - Setting to Default" + DEFAULT_PARTITION_FIELD_NAME);
			partitionFieldName = DEFAULT_PARTITION_FIELD_NAME;
		}
	}

	private ByteBuffer getByteBuffer(byte[] payload) {
		ByteBuffer buffer = ByteBuffer.wrap(payload);
		byte magic = buffer.get();
		logger.debug("MAGIC BYTE" + magic);
		if (magic != MAGIC_BYTE) {
			throw new MessageDecoderException("Unknown magic byte!");
		}
		return buffer;
	}

	private String constructSubject(String topic, Schema schema, boolean isNewProducer) {
		if (isNewProducer) {
			return topic + "-value";
		} else {
			return schema.getName() + "-value";
		}
	}

	private Object deserialize(byte[] payload) throws MessageDecoderException {
		try {
			if (payload == null) {
				return null;
			}
			ByteBuffer buffer = getByteBuffer(payload);
			int id = buffer.getInt();
			Schema schema = schemaRegistry.getByID(id);
			if (schema == null)
				throw new IllegalStateException("Unknown schema id: " + id);
			logger.info("Schema = " + schema.toString());
			String subject = constructSubject(topic, schema, isNew);
			logger.info("Subject = " + subject);

			// We need to initialize latestSchema and latestVersion here
			// to handle both old and new producers as we don't know
			// the Avro record name yet during decoder creation.
			if (latestSchema == null) {
				SchemaMetadata metadata = schemaRegistry.getLatestSchemaMetadata(subject);
				latestSchema = new Schema.Parser().parse(metadata.getSchema());
				latestVersion = metadata.getVersion();
			}

			int version = schemaRegistry.getVersion(subject, schema);
			if (version > latestVersion) {
				String errorMsg = String.format("Producer schema is newer than the schema known to Camus");
				throw new MessageDecoderException(errorMsg);
			}

			int length = buffer.limit() - 1 - idSize;
			if (schema.getType().equals(Schema.Type.BYTES)) {
				byte[] bytes = new byte[length];
				buffer.get(bytes, 0, length);
				return bytes;
			}
			int start = buffer.position() + buffer.arrayOffset();
			DatumReader<Object> reader = new GenericDatumReader<Object>(schema, latestSchema);
			Object object = reader.read(null, decoderFactory.binaryDecoder(buffer.array(), start, length, null));

			if (schema.getType().equals(Schema.Type.STRING)) {
				object = ((Utf8) object).toString();
			}
			return object;
		} catch (IOException ioe) {
			throw new MessageDecoderException("Error deserializing Avro message", ioe);
		} catch (RestClientException re) {
			throw new MessageDecoderException("Error deserializing Avro message", re);
		}
	}

	public CamusWrapper<Record> decode(byte[] payload) {
		Object object = deserialize(payload);
		if (object instanceof Record) {
			return new CamusAvroWrapper((Record) object);
		} else {
			throw new MessageDecoderException("Camus does not support Avro primitive types!");
		}
	}

	static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	public static class CamusAvroWrapper extends CamusWrapper<Record> {
		public CamusAvroWrapper(Record record) {
			super(record);
			logger.info("Printing record----" + super.getRecord().get(partitionFieldName));
			System.out.println("Printing record----" + super.getRecord().get(partitionFieldName));
			GenericData.Record header = (Record) super.getRecord().get("header");
			if (header != null) {
				if (header.get("server") != null) {
					put(new Text("server"), new Text(header.get("server").toString()));
				}
				if (header.get("service") != null) {
					put(new Text("service"), new Text(header.get("service").toString()));
				}
			}
			Long loadDate = new Long(super.getRecord().get(partitionFieldName).toString());
			
			Date date = new Date(loadDate);
			System.out.println("Long value is - " + loadDate + " and Date Value is - " + date);
			put(new Text("load_date"), new Text(sdf.format(date)));
		}

		@Override
		public long getTimestamp() {
			Record header = (Record) super.getRecord().get("header");
			if (header != null && header.get("time") != null) {
				return (Long) header.get("time");
			} else if (super.getRecord().get("timestamp") != null) {
				return (Long) super.getRecord().get("timestamp");
			} else {
				return System.currentTimeMillis();
			}
		}
	}
}
