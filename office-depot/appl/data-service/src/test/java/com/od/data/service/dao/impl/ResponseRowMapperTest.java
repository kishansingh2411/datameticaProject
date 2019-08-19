package com.od.data.service.dao.impl;

import static org.junit.Assert.assertEquals;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.dao.impl.ResponseRowMapper;
import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ColumnHeader;
import com.od.data.service.rest.domain.DataType;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class ResponseRowMapperTest {

  private static final String ROW_VALUE = "201402";
  private static final String ERROR_ROW_VALUE = "20141010";
  private static final Integer ROW_VALUE_INT = 1;
  private static final Long ROW_VALUE_LONG = 1l;
  private static final Double ROW_VALUE_DOUBLE = 1.0;
  private static final Boolean ROW_VALUE_BOOLEAN = true;
  private ResponseRowMapper responseRowMapper;
  private ColumnHeader columnHeaderMock;

  @Before
  public void setUp() throws Exception {
    ArrayList<ColumnHeader> columnHeaders = new ArrayList<ColumnHeader>();
    columnHeaderMock = Mockito.mock(ColumnHeader.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.DATE);
    columnHeaders.add(columnHeaderMock);

    responseRowMapper = new ResponseRowMapper(columnHeaders, AggregatorLevel.QUARTER);
  }

  @Test
  public void shouldMapRow() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.DATE);
    Mockito.when(rsMock.getString(1)).thenReturn(ROW_VALUE);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("2014Q02", mapRow[0]);
    assertEquals(1, mapRow.length);
  }

  @Test(expected = SQLException.class)
  public void shouldThrowExceptionOnParseError() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.DATE);
    Mockito.when(rsMock.getString(1)).thenReturn(ERROR_ROW_VALUE);

    responseRowMapper.mapRow(rsMock, 1);
  }

  @Test
  public void shouldMapRowWhenDataTypeNotDate() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.STRING);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.STRING);
    Mockito.when(rsMock.getString(1)).thenReturn(ROW_VALUE);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("201402", mapRow[0]);
    assertEquals(1, mapRow.length);
  }
  @Test
  public void shouldMapRowWhenDataTypeInteger() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.INTEGER);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.INTEGER);
    Mockito.when(rsMock.getInt(1)).thenReturn(ROW_VALUE_INT);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("1", mapRow[0]);
    assertEquals(1, mapRow.length);
  }
  @Test
  public void shouldMapRowWhenDataTypeLong() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.LONG);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.LONG);
    Mockito.when(rsMock.getLong(1)).thenReturn(ROW_VALUE_LONG);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("1", mapRow[0]);
    assertEquals(1, mapRow.length);
  }
  @Test
  public void shouldMapRowWhenDataTypeDouble() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.DOUBLE);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.DOUBLE);
    Mockito.when(rsMock.getDouble(1)).thenReturn(ROW_VALUE_DOUBLE);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("1.000000", mapRow[0]);
    assertEquals(1, mapRow.length);
  }
  @Test
  public void shouldMapRowWhenBooleanReader() throws SQLException {
    ResultSet rsMock = Mockito.mock(ResultSet.class);
    Mockito.when(columnHeaderMock.getDataType()).thenReturn(DataType.BOOLEAN);
    Mockito.when(columnHeaderMock.getColumnDataType()).thenReturn(DataType.BOOLEAN);
    Mockito.when(rsMock.getBoolean(1)).thenReturn(ROW_VALUE_BOOLEAN);

    String[] mapRow = responseRowMapper.mapRow(rsMock, 1);

    assertEquals("true", mapRow[0]);
    assertEquals(1, mapRow.length);
  }
}
