package com.od.data.service.dao.impl;

import static org.junit.Assert.assertSame;

import java.util.ArrayList;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.dao.impl.BaseDaoImpl;
import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ColumnHeader;
import com.od.data.service.rest.domain.ColumnType;
import com.od.data.service.rest.domain.DataType;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class BaseDaoImplTest {

  private BaseDaoImpl baseDaoImpl;
  private JdbcTemplate jdbcTemplateMock;

  @Before
  public void setUp() {
    baseDaoImpl = new BaseDaoImpl();
    jdbcTemplateMock = Mockito.mock(JdbcTemplate.class);
    ReflectionTestUtils.setField(baseDaoImpl, "jdbcTemplate", jdbcTemplateMock);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetRows() {

    List<String[]> mockRows = getMockRows();
    Mockito.when(jdbcTemplateMock.query(Mockito.anyString(), (RowMapper<String[]>) Mockito.anyObject())).thenReturn(mockRows);

    List<String[]> rows = baseDaoImpl.getRows("abc", new ArrayList<ColumnHeader>(), AggregatorLevel.DAY);

    assertSame(mockRows, rows);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetRowsWhenAggregatedByQuarter() {

    List<String[]> mockRows = getMockRows();
    Mockito.when(jdbcTemplateMock.query(Mockito.anyString(), (RowMapper<String[]>) Mockito.anyObject())).thenReturn(mockRows);

    ArrayList<ColumnHeader> columnHeaders = new ArrayList<ColumnHeader>();
    columnHeaders.add(new ColumnHeader("date", ColumnType.DIMENSION, DataType.DATE, DataType.DATE));
    List<String[]> rows = baseDaoImpl.getRows("abc", columnHeaders, AggregatorLevel.QUARTER);

    assertSame(mockRows, rows);
  }

  private List<String[]> getMockRows() {
    List<String[]> mockRows = new ArrayList<String[]>();
    return mockRows;
  }

}
