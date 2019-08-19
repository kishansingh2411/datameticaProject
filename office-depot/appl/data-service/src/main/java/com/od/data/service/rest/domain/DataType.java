package com.od.data.service.rest.domain;

import com.od.data.service.util.ResultSetValueReaders;
import com.od.data.service.util.ResultSetValueReaders.ResultSetValueReader;

/**
 * @author sandeep
 * 
 */
public enum DataType {
  INTEGER(Boolean.FALSE, ResultSetValueReaders.INTEGER_VALUE_READER), DOUBLE(Boolean.FALSE,
      ResultSetValueReaders.DOUBLE_VALUE_READER), STRING(Boolean.TRUE,
      ResultSetValueReaders.STRING_VALUE_READER), LONG(Boolean.FALSE,
      ResultSetValueReaders.LONG_VALUE_READER), DATE(Boolean.FALSE,
      ResultSetValueReaders.STRING_VALUE_READER),BOOLEAN(Boolean.FALSE,
    	      ResultSetValueReaders.BOOLEAN_VALUE_READER);

  private DataType(Boolean needQuotes, ResultSetValueReader resultSetValueReader) {
    this.needQuotes = needQuotes;
    this.resultSetValueReader = resultSetValueReader;
  }

  private Boolean needQuotes;
  private final ResultSetValueReader resultSetValueReader;

  public Boolean getNeedQuotes() {
    return needQuotes;
  }

  public ResultSetValueReader getResultSetValueReader() {
    return resultSetValueReader;
  }

}
