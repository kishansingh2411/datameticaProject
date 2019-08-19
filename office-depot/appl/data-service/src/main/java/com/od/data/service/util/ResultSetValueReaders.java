package com.od.data.service.util;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DecimalFormat;

/**
 * @author sandeep
 * 
 */
public class ResultSetValueReaders {

  public interface ResultSetValueReader {
    public String readValue(ResultSet rs, int columnIndex) throws SQLException;
  }

  public final static ResultSetValueReader INTEGER_VALUE_READER = new ResultSetValueReader() {
    @Override
    public String readValue(ResultSet rs, int columnIndex) throws SQLException {
      return String.valueOf(rs.getInt(columnIndex));
    }
  };

  public final static ResultSetValueReader LONG_VALUE_READER = new ResultSetValueReader() {
    @Override
    public String readValue(ResultSet rs, int columnIndex) throws SQLException {
      return String.valueOf(rs.getLong(columnIndex));
    }
  };

  public final static ResultSetValueReader DOUBLE_VALUE_READER = new ResultSetValueReader() {
    private static final String FORMAT_KEY = "resultsetvaluereaders.doublevaluereader.format";
    private final DecimalFormat formatter = new DecimalFormat(ConfigUtil.getProperty(FORMAT_KEY));

    @Override
    public String readValue(ResultSet rs, int columnIndex) throws SQLException {
      return formatter.format(rs.getDouble(columnIndex));
    }
  };

  public final static ResultSetValueReader STRING_VALUE_READER = new ResultSetValueReader() {
    @Override
    public String readValue(ResultSet rs, int columnIndex) throws SQLException {
      return rs.getString(columnIndex);
    }
  };

  public final static ResultSetValueReader BOOLEAN_VALUE_READER = new ResultSetValueReader() {
    @Override
    public String readValue(ResultSet rs, int columnIndex) throws SQLException {
      return String.valueOf(rs.getBoolean(columnIndex));
    }
  };

}
