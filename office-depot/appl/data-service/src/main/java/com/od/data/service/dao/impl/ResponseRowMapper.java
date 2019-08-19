package com.od.data.service.dao.impl;

import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ColumnHeader;
import com.od.data.service.rest.domain.DataType;
import com.od.data.service.util.QuarterDateFormat;
import com.od.data.service.util.ResultSetValueReaders.ResultSetValueReader;

import org.apache.log4j.Logger;
import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;


/**
 * @author sandeep
 *
 */
public class ResponseRowMapper implements RowMapper<String[]> {

    private DateFormat apiDateFormat;
    private DateFormat hbaseDateFormat;
    private List<ColumnHeader> columnHeaders;
    private static final Logger LOGGER = Logger.getLogger(ResponseRowMapper.class);

    public ResponseRowMapper(List<ColumnHeader> columnHeaders, AggregatorLevel aggregatorLevel) {
      this.columnHeaders = columnHeaders;
      if (AggregatorLevel.QUARTER == aggregatorLevel) {
        this.apiDateFormat = new QuarterDateFormat(aggregatorLevel.getResponseDateFormat());
        this.hbaseDateFormat = new QuarterDateFormat(aggregatorLevel.getQueryColumnFormat());
      } else {
        this.apiDateFormat = new SimpleDateFormat(aggregatorLevel.getResponseDateFormat());
        this.hbaseDateFormat = new SimpleDateFormat(aggregatorLevel.getQueryColumnFormat());
      }

      for (ColumnHeader columnHeader : columnHeaders) {
        if (DataType.DATE == columnHeader.getDataType()) {
          break;
        }
      }
    }

    @Override
    public String[] mapRow(ResultSet rs, int rowNum) throws SQLException {
      int columnCount = columnHeaders.size();
      String[] row = new String[columnCount];
      int i = 1;
      for (ColumnHeader columnHeader : columnHeaders) {
        DataType dataType = columnHeader.getDataType();
        DataType columnDataType = columnHeader.getColumnDataType();
        ResultSetValueReader rsValueReader = columnDataType.getResultSetValueReader();
        String rowValue = rsValueReader.readValue(rs, i);

        if (dataType == DataType.DATE) {
          rowValue = transformDateRowValue(rowValue);
        }
        row[i - 1] = rowValue;
        i++;
      }

      return row;
    }

    private String transformDateRowValue(String rowValue) throws SQLException {
      try {
        Date date = hbaseDateFormat.parse(rowValue);
        rowValue = apiDateFormat.format(date);
      } catch (ParseException e) {
        LOGGER.error("Error parsing date returned from HBase: " + rowValue);
        throw new SQLException(e);
      }
      return rowValue;
    }

  }
