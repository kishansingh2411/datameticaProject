package com.od.data.service.rest.domain;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

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

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class QueryTest {

  private Query query;
  private Request requestMock;

  @Before
  public void setUp() throws Exception {
    requestMock = Mockito.mock(Request.class);
    Mockito.when(requestMock.getFromDate()).thenReturn("2014-07-01");
    Mockito.when(requestMock.getToDate()).thenReturn("2014-07-05");
    Mockito.when(requestMock.getDimensions()).thenReturn("country,visitorSegment");
    Mockito.when(requestMock.getMetrics()).thenReturn("totalOrders,totalRevenue");
    List<String> filters = new ArrayList<String>();
    filters.add("country eq UK, visitorSegment eq abc");
    Mockito.when(requestMock.getFilters()).thenReturn(filters);
    Mockito.when(requestMock.getLimit()).thenReturn(null);
    Mockito.when(requestMock.getModule()).thenReturn(Module.CLICKSTREAM);

  }

  @Test
  public void shouldGetDimensions() {
    Mockito.when(requestMock.getDimensions()).thenReturn("country,visitorSegment,date");

    query = new Query(requestMock, Boolean.TRUE);
    List<Dimension> dimensions = query.getDimensions();

    assertEquals(3, dimensions.size());
    assertEquals("country", dimensions.get(0).getName());
  }

  @Test
  public void shouldGetMetrics() {
    query = new Query(requestMock, Boolean.TRUE);
    List<Metric> metrics = query.getMetrics();

    assertEquals(2, metrics.size());
    assertEquals("totalOrders", metrics.get(0).getName());
  }

  @Test
  public void shouldGetDimensionFilters() {
    query = new Query(requestMock, Boolean.TRUE);
    List<Filter> dimensionFilters = query.getDimensionFilters();

    assertEquals(1, dimensionFilters.size());
    assertEquals("country", dimensionFilters.get(0).getName());
  }

  @Test
  public void shouldGetFromDate() {
    query = new Query(requestMock, Boolean.TRUE);
    String fromDate = query.getFromDate();

    assertEquals("20140701", fromDate);
  }

  @Test
  public void shouldGetToDate() {
    query = new Query(requestMock, Boolean.TRUE);
    String toDate = query.getToDate();

    assertEquals("20140705", toDate);
  }

  @Test
  public void shouldAggregatorLevelAsSpecified() {
    Mockito.when(requestMock.getDimensions()).thenReturn("country,visitorSegment");
    Mockito.when(requestMock.getFromDate()).thenReturn("2014Q01");
    Mockito.when(requestMock.getToDate()).thenReturn("2014Q02");
    query = new Query(requestMock, Boolean.TRUE);

    assertEquals("yyyy'Q'qq", query.getAppliedDatePattern());
    assertEquals(AggregatorLevel.QUARTER, query.getAggregatorLevel());
  }
}
