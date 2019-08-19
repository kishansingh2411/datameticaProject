package com.clickstream.avroserializer;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.serialization.AbstractAvroEventSerializer;
import org.apache.flume.serialization.EventSerializer;
import org.apache.log4j.Logger;

/**
 * @author Sonali Rawool
 *
 */
public class AvroEventSerializer extends AbstractAvroEventSerializer<GenericRecord> {
	
	
	private static final Logger LOGGER = Logger.getLogger(AvroEventSerializer.class);
	
	private static String schemaFile;
	private Schema schema;
	private final OutputStream out;
	private AvroRecordConverter converter = null;

	private AvroEventSerializer(OutputStream out) {
		this.schema = readSchema(schemaFile);
		this.out = out;
		converter = new AvroRecordConverter();
	}

	protected Schema readSchema(String schemaFile) {
		try {
			return new Schema.Parser().parse(new File(schemaFile));
		} catch (Exception e) {
			LOGGER.error("Error occured while reading the schema file "+schemaFile, e);
		}
		return null;
	}

	@Override
	protected GenericRecord convert(Event event) {
		String data =  new String(event.getBody());
		GenericRecord record = null;
		try{
			if(!StringUtils.isEmpty(data)){
				record = converter.convertRecordInAvroFormat(schema, data);
			}
		}catch(Exception e){
			LOGGER.error("Error occured while convert the event "+event, e);
		}
		return record;
	}

	@Override
	public void write(Event event) throws IOException {
		String data =  new String(event.getBody());
		if(!StringUtils.isEmpty(data)){
			super.write(event);
		}
	}

	@Override
	protected OutputStream getOutputStream() {
		return out;
	}

	@Override
	protected Schema getSchema() {
		return schema;
	}

	public static class Builder implements EventSerializer.Builder {

		private static final String SCHEMA_FILE = "schema_file";

		public EventSerializer build(Context context, OutputStream out) {
			schemaFile = context.getString(SCHEMA_FILE);
			AvroEventSerializer writer = new AvroEventSerializer(out);
			writer.configure(context);
			return writer;
		}
	}

}
