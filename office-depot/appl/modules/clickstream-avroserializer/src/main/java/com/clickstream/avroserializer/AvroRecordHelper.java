package com.clickstream.avroserializer;

import java.io.IOException;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;

/**
 * This is an utility class which helps in writing Avro format records.
 * 
 * @author sandeep
 * 
 */
public class AvroRecordHelper {

  /**
   * This method loads the given avro schema file and return the Schema object. File name should be
   * passed along with fully qualified path.
   * 
   * @param schemaFileWithPath
   * @return
   * @throws IOException
   */
  public static Schema loadAvroSchema(String schemaFileWithPath) throws IOException {

//    Schema schema = new Schema.Parser().parse(new File(schemaFileWithPath));
    Schema schema = new Schema.Parser().parse(Thread.currentThread().getContextClassLoader().getResourceAsStream("message.avsc"));


    return schema;
  }

  /**
   * This method converts the passed String value to the appropriate Type defined in the schema for
   * the corresponding key passed.
   * 
   * @param schema
   * @param key
   * @param value
   * @return
   */
  public static Object convertValueStringToAvroKeyType(Schema schema, String key, String value) {
    Type type = null;
    if (schema.getType() == Type.RECORD && schema.getField(key) != null) {
        type = schema.getField(key).schema().getType();
      } else {
        type = schema.getType();
      }
    Object convertedValue = null;
    if (type == Type.UNION) {
      convertedValue = resolveUnionAndConvertValueToAvroKeyType(schema, key, value);
    } else {
      convertedValue = convertPrimitiveValueToAvroKeyType(type, key, value);
    }
    return convertedValue;
  }

  private static Object convertPrimitiveValueToAvroKeyType(Type type, String key, String value) {
    Object newValue = value;
    switch (type) {
      case BOOLEAN:
        newValue = Boolean.parseBoolean(value);
        break;
      case DOUBLE:
        newValue = Double.parseDouble(value);
        break;
      case FLOAT:
        newValue = Float.parseFloat(value);
        break;
      case INT:
        newValue = Integer.parseInt(value);
        break;
      case LONG:
        newValue = Long.parseLong(value);
        break;
      case STRING:
        newValue = value;
        break;
      default:
        newValue = value;
    }
    return newValue;
  }

  private static Object resolveUnionAndConvertValueToAvroKeyType(Schema schema, String key,
      String value) {
    Schema unionSchema = schema.getField(key).schema();
    List<Schema> types = unionSchema.getTypes();
    Object convertedValue = null;
    for (int i = 0; i < types.size(); i++) {
      try {
        if (types.get(i).getType() == Type.NULL) {
          if (value == null || value.equals("null")) {
            convertedValue = null;
            break;
          } else {
            continue;
          }
        }
        convertedValue = convertPrimitiveValueToAvroKeyType(types.get(i).getType(), key, value);
      } catch (RuntimeException e) {
        continue;
      }
      break;
    }
    return convertedValue;
  }
}
