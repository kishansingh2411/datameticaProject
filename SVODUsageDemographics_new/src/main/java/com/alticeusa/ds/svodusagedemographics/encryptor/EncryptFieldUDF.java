package com.alticeusa.ds.svodusagedemographics.encryptor;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * Java class creating a Hive UDF function to encrypt a given field.
 * Created by ksingh on 5/5/16.
 */
@Description(
    name="EncryptFieldUDF",
    value="returns encrypted(value), where value is the given string",
    extended="SELECT encryptField(field) from foo limit 1;"
)
public class EncryptFieldUDF extends UDF
{
  //private static final String DEV_USER = "ksingh5";
  private static final String UAT_USER = "etlmgr";
  private static final String DEVICE = "VOD001";

  private CVDataController cvc;

  public EncryptFieldUDF( )
  {
    try {
      cvc = new CVDataController(DEVICE, UAT_USER);
    } catch( CVApplicationException e)
    {
      System.out.println( "DEBUG: DEVICE = " + DEVICE + "\t USER = " + UAT_USER);
      throw new RuntimeException( e);
    }
  }

  public String evaluate(String input)
  {
    if(input == null) return null;

    try
    {
      String encryptedValue = cvc.encryptField("device", input);
      return encryptedValue;
    }
    catch( CVApplicationException e)
    {
      System.out.println( "DEBUG: Encryption Exception on = " + input);
      throw new RuntimeException (e);
    }
  }
}
