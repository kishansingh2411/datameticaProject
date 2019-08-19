package com.od.data.service.util;

import org.junit.Before;
import org.junit.Test;

import com.od.data.service.util.QuarterDateFormat;

/**
 * @author sandeep
 * 
 */
public class QuarterDateFormatTest {

  @Before
  public void setUp() throws Exception {
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnInvalidPattern() {

    new QuarterDateFormat("2014p");
  }

}
