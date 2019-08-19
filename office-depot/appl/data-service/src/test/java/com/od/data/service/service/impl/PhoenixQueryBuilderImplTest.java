package com.od.data.service.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
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
import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.Query;
import com.od.data.service.rest.domain.Request;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class PhoenixQueryBuilderImplTest {

  private PhoenixQueryBuilderImpl phoenixQueryBuilderImpl;

  @Before
  public void setup() {
    phoenixQueryBuilderImpl = new PhoenixQueryBuilderImpl();
  }

  @Test
  public void testBuildQueryForDailyViewWithLimit() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015-05-01", "2014-05-06",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_daily where day>=20150501 and day<=20140506 group by country,visitor_segment limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);

  }

  @Test
  public void testBuildQueryForDailyViewWithZeroLimit() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015-05-01", "2014-05-06",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), 0);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_daily where day>=20150501 and day<=20140506 group by country,visitor_segment";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery.trim());

  }

  @Test
  public void testBuildQueryForDailyViewWithNullLimit() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015-05-01", "2014-05-06",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), null);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_daily where day>=20150501 and day<=20140506 group by country,visitor_segment";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery.trim());

  }

  @Test
  public void testBuildQueryForWeeklyView() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015W01", "2011W05",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_weekly where week>=201501 and week<=201105 group by country,visitor_segment limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);
  }

  @Test
  public void testBuildQueryForMonthlyView() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015-01", "2015-05",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_monthly where month>=201501 and month<=201505 group by country,visitor_segment limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);
  }

  @Test
  public void testBuildQueryForQuarterlyView() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2015Q01", "2015Q04",
        "country,visitorSegment", "totalOrders,totalRevenue", new ArrayList<String>(0), 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_quarterly where quarter>=201501 and quarter<=201504 group by country,visitor_segment limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);
  }

  @Test
  public void testBuildQueryWhenNoDimensionProvided() {
    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2014-07-01", "2014-07-05", "",
        "totalRevenue", new ArrayList<String>(0), 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_daily where day>=20140701 and day<=20140705 limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);

  }

  @Test
  public void testBuildQueryWhenDimensionFiltersProvided() {
    List<String> filters = new ArrayList<String>();
    filters.add("country eq UK, visitorSegment lt abc");

    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2014-07-01", "2014-07-05",
        "country,visitorSegment", "totalOrders,totalRevenue", filters, 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertTrue(actualQuery.contains("(country='UK' or visitor_segment<'abc' )"));
  }

  @Test
  public void testBuildQueryWhenMetricsFiltersProvided() {
    List<String> filters = new ArrayList<String>();
    filters.add("totalOrders gt 120");

    ClickStreamRequest clickStreamRequest = createRequestMock(ClickStreamRequest.class, Module.CLICKSTREAM, "2014-07-01", "2014-07-05",
        "country,visitorSegment", "totalOrders,totalRevenue", filters, 10);

    Query query = new Query(clickStreamRequest, Boolean.TRUE);

    String expectedResult = "select country,visitor_segment,sum(total_orders),sum(total_REV_FRM_NCUST+total_REV_FRM_ECUST+total_REV_FRM_ANYMCUST) from clickstream_view_daily where day>=20140701 and day<=20140705 group by country,visitor_segment having (sum(total_orders)>120 and sum(total_orders) is not null)  limit 10";

    String actualQuery = phoenixQueryBuilderImpl.buildQuery(query);

    Assert.assertEquals(expectedResult, actualQuery);
  }

  private <T extends Request> T createRequestMock(Class<T> class1, Module module, String fromDate, String toDate, String dimensions,
      String metrics, List<String> filters, Integer limit) {
    T request = Mockito.mock(class1);
    Mockito.when(request.getFromDate()).thenReturn(fromDate);
    Mockito.when(request.getToDate()).thenReturn(toDate);
    Mockito.when(request.getDimensions()).thenReturn(dimensions);
    Mockito.when(request.getMetrics()).thenReturn(metrics);
    Mockito.when(request.getFilters()).thenReturn(filters);
    Mockito.when(request.getLimit()).thenReturn(limit);
    Mockito.when(request.getModule()).thenReturn(module);

    return request;
  }

}
