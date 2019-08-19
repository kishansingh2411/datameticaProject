package com.od.data.service.rest.controller;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.util.ReflectionTestUtils;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.rest.controller.ClickStreamController;
import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.service.ClickStreamService;
import com.od.data.service.util.DimensionUtil;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@PrepareForTest({DimensionUtil.class})
@WebAppConfiguration
public class ClickStreamControllerTest {

  ClickStreamController clickStreamController;
  ClickStreamService clickStreamServiceMock;
  ClickStreamRequest clickStreamRequestMock;

  @Before
  public void setUp() {
    clickStreamController = new ClickStreamController();
    clickStreamServiceMock = Mockito.mock(ClickStreamService.class);
    ReflectionTestUtils.setField(clickStreamController, "clickStreamService", clickStreamServiceMock);
    clickStreamRequestMock = Mockito.mock(ClickStreamRequest.class);
  }

  @Test
  public void testGetPhoenixJdbcQuery() {
    clickStreamController.getPhoenixJdbcQuery(clickStreamRequestMock);
    Mockito.verify(clickStreamServiceMock, Mockito.times(1)).getJDBCQuery(clickStreamRequestMock);
  }

  @Test
  public void shouldDelegateToClickStreamService() {
    clickStreamController.getRecords(clickStreamRequestMock);
    Mockito.verify(clickStreamServiceMock, Mockito.times(1)).getRecordsForClickStream(clickStreamRequestMock);

  }

  @Test
  public void shouldCallDimensionUtilForSupportedDimensions() {
    clickStreamController.getSupportedDimensions();
  }

  @Test
  public void shouldCallMetricUtilforSupportedMetrics() {
    clickStreamController.getSupportedMetrics();
  }
}
