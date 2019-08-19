package com.od.data.service.service.impl;

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
import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.ColumnHeader;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.Query;
import com.od.data.service.service.QueryBuilder;
import com.od.data.service.service.impl.ClickStreamServiceImpl;

/**
 * @author sandeep
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class ClickStreamServiceImplTest {

  private QueryBuilder queryBuilder;
  private BaseDao clickStreamDao;
  private ClickStreamServiceImpl clickStreamServiceImpl;

  @Before
  public void setup() {
    queryBuilder = Mockito.mock(QueryBuilder.class);
    clickStreamDao = Mockito.mock(BaseDao.class);
    clickStreamServiceImpl = new ClickStreamServiceImpl();
    clickStreamServiceImpl.setQueryBuilder(queryBuilder);
    clickStreamServiceImpl.setClickStreamDao(clickStreamDao);
  }
  @Test
  public void testGetResults() {

    ClickStreamRequest clickStreamRequest = Mockito.mock(ClickStreamRequest.class);
    Mockito.when(clickStreamRequest.getFromDate()).thenReturn("20140701");
    Mockito.when(clickStreamRequest.getToDate()).thenReturn("20140705");
    Mockito.when(clickStreamRequest.getDimensions()).thenReturn("country,visitorSegment");
    Mockito.when(clickStreamRequest.getMetrics()).thenReturn("totalOrders,totalRevenue");
    Mockito.when(clickStreamRequest.getFilters()).thenReturn(new ArrayList<String>(0));
    Mockito.when(clickStreamRequest.getLimit()).thenReturn(10);
    Mockito.when(clickStreamRequest.getModule()).thenReturn(Module.CLICKSTREAM);

    Mockito.when(queryBuilder.buildQuery(Mockito.any(Query.class))).thenReturn("");
    Mockito.when(clickStreamDao.getRows(Mockito.anyString(), Mockito.anyListOf(ColumnHeader.class), Mockito.any(AggregatorLevel.class)))
        .thenReturn(new ArrayList<String[]>(0));

    clickStreamServiceImpl.getRecordsForClickStream(clickStreamRequest);

    Mockito.verify(queryBuilder, Mockito.times(1)).buildQuery(Mockito.any(Query.class));
    Mockito.verify(clickStreamDao, Mockito.times(1)).getRows(Mockito.anyString(), Mockito.anyListOf(ColumnHeader.class),
        Mockito.any(AggregatorLevel.class));

  }

  @Test
  public void testGetResultsWithOrFilter() {
    ClickStreamRequest clickStreamRequest = Mockito.mock(ClickStreamRequest.class);
    Mockito.when(clickStreamRequest.getFromDate()).thenReturn("20140701");
    Mockito.when(clickStreamRequest.getToDate()).thenReturn("20140705");
    Mockito.when(clickStreamRequest.getDimensions()).thenReturn("country,visitorSegment");
    Mockito.when(clickStreamRequest.getMetrics()).thenReturn("totalOrders,totalRevenue");

    List<String> filterList = new ArrayList<String>(2);
    filterList.add("country EQ UK, visitorSegment EQ abc");
    filterList.add("country EQ 123");

    Mockito.when(clickStreamRequest.getFilters()).thenReturn(filterList);
    Mockito.when(clickStreamRequest.getLimit()).thenReturn(10);
    Mockito.when(clickStreamRequest.getModule()).thenReturn(Module.CLICKSTREAM);

    Mockito.when(queryBuilder.buildQuery(Mockito.any(Query.class))).thenReturn("");
    Mockito.when(clickStreamDao.getRows(Mockito.anyString(), Mockito.anyListOf(ColumnHeader.class), Mockito.any(AggregatorLevel.class)))
        .thenReturn(new ArrayList<String[]>(0));

    clickStreamServiceImpl.getRecordsForClickStream(clickStreamRequest);

    Mockito.verify(queryBuilder, Mockito.times(1)).buildQuery(Mockito.any(Query.class));
    Mockito.verify(clickStreamDao, Mockito.times(1)).getRows(Mockito.anyString(), Mockito.anyListOf(ColumnHeader.class),
        Mockito.any(AggregatorLevel.class));
  }
}
