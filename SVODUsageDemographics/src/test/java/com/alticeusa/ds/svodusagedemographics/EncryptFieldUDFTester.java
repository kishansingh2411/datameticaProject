package com.alticeusa.ds.svodusagedemographics;

import com.alticeusa.ds.svodusagedemographics.encryptor.EncryptFieldUDF;
import junit.framework.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * Created by ksingh on 5/5/16.
 */
public class EncryptFieldUDFTester
{
  @Test
  public void testUDF( )
  {
    EncryptFieldUDF obj = new EncryptFieldUDF();
    assertEquals( "9287CCBA38087F6E07253595F8785600", obj.evaluate( "Test"));
  }

  @Test
  public void testUDFNullCheck( )
  {
    EncryptFieldUDF obj = new EncryptFieldUDF();
    assertNull(obj.evaluate(null));
  }

}
