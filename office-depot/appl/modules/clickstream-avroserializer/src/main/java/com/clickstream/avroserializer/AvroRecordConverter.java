package com.clickstream.avroserializer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;

/**
 * This class helps us in converting  Json record to Avro format.
 *
 * @author sandeep
 *
 */
public class AvroRecordConverter {
	
	private static final Logger LOGGER = Logger.getLogger(AvroRecordConverter.class);
	
  /**
   * This method converts the Json String logger record to Avro Format.
   *
   * @param record
   * @return
   * @throws IOException
   */
  public GenericRecord convertRecordInAvroFormat( Schema avroSchema, String record) throws IOException {
    Object jsonMap = null;
    try{
      jsonMap = loadJsonPayloadStringInMap(record);
    }catch(Exception e){
    	LOGGER.error("Error occured while record to avro format "+record, e);
    }

    if (jsonMap != null) {
      Map<String, Object> jsonPayloadStringInMap = (Map<String, Object>) jsonMap;
      GenericRecord avroFormattedRecord =
          (GenericRecord) writeMapToAvro(avroSchema, null, jsonPayloadStringInMap);
      return avroFormattedRecord;
    }
    return null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> loadJsonPayloadStringInMap(String jsonString) {
    Object jsonMap = fromJsonToMap(new JsonParser().parse(jsonString));
    return jsonMap instanceof JsonNull ? null : (Map<String, Object>) jsonMap;
  }

  private Map<String, Object> toMap(JsonObject object) {
    Map<String, Object> map = new HashMap<String, Object>();
    Set<Map.Entry<String, JsonElement>> set = object.entrySet();
    Iterator<Map.Entry<String, JsonElement>> iterator = set.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, JsonElement> entry = iterator.next();
      String key = entry.getKey();
      map.put(key, fromJsonToMap(entry.getValue()));
    }
    return map;
  }

  private List<Object> toList(JsonArray array) {
    List<Object> list = new ArrayList<Object>();
    for (int i = 0; i < array.size(); i++) {
      list.add(fromJsonToMap(array.get(i)));
    }
    return list;
  }

  private Object fromJsonToMap(Object json) {
    if (json == null) {
      return null;
    } else if (json instanceof JsonObject) {
      return toMap((JsonObject) json);
    } else if (json instanceof JsonArray) {
      return toList((JsonArray) json);
    } else if (json instanceof JsonPrimitive) {
      return ((JsonPrimitive) json).getAsString();
    }
    return json;
  }

  @SuppressWarnings("unchecked")
  public Object writeMapToAvro(Schema schema, String key, Object object) {
    if (object == null) {
      return null;
    } else if (object instanceof Map) {
      return fromMap(schema, key, (Map<String, Object>) object);
    } else if (object instanceof List) {
      return fromList(schema, key, (List<Object>) object);
    } else {
      Object value =
          AvroRecordHelper.convertValueStringToAvroKeyType(schema, key, object.toString());
      return value;
    }
  }

  private Object fromMap(Schema schema, String key, Map<String, Object> object) {
    GenericRecord avroRec;
    avroRec = getAvroRecord(schema, key);
    Set<Map.Entry<String, Object>> set = object.entrySet();
    Iterator<Map.Entry<String, Object>> iterator = set.iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, Object> entry = iterator.next();
      String key1 = entry.getKey();
      Object value = null;
      try {
        value = writeMapToAvro(avroRec.getSchema(), key1, entry.getValue());
        avroRec.put(key1, value);
      } catch (AvroRuntimeException e) {
        // Skip and move to next
      }
    }
    return avroRec;
  }

  private GenericRecord getAvroRecord(Schema schema, String key) {
    GenericRecord genericRecord = null;
    if (key == null) {
      genericRecord = new GenericData.Record(schema);
    } else if (schema.getType() == Type.ARRAY) {
      genericRecord = new GenericData.Record(schema.getElementType());
    } else if (schema.getType() == Type.UNION
        || (schema.getField(key) != null && schema.getField(key).schema().getType() == Type.UNION)) {
      List<Schema> schemaList = schema.getField(key).schema().getTypes();
      for (Schema keySchema : schemaList) {
        if (keySchema.getType() == Type.NULL) {
          continue;
        } else if (keySchema.getType() == Type.RECORD) {
          genericRecord = new GenericData.Record(keySchema);
          break;
        }
      }

    }
    return genericRecord == null ? new GenericData.Record(schema) : genericRecord;
  }

  private Object fromList(Schema schema, String key, List<Object> object) {
    GenericArray<Object> genericArray = null;
    if (isFieldExistInSchema(schema, key)) {
      List<Schema> schemaList = schema.getField(key).schema().getTypes();
      for (Schema keySchema : schemaList) {
        if (keySchema.getType() == Type.NULL) {
          continue;
        } else if (keySchema.getType() == Type.ARRAY) {
          genericArray = new GenericData.Array<Object>(object.size(),
              keySchema);
          schema = keySchema.getElementType();
        }
      }
      for (int i = 0; i < object.size(); i++) {
        genericArray.add(writeMapToAvro(schema, key, object.get(i)));
      }
    }
    return genericArray;
  }
  private boolean isFieldExistInSchema(Schema schema, String key) {
    if(schema.getField(key) != null){
      return true;
    }
    return false;
  }

}

