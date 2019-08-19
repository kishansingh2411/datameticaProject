package com.od.data.service.rest.controller;

import java.util.Collection;

import javax.validation.Valid;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

//import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.CoverageAnalyticsRequest;
import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Metric;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.Response;
//import com.od.data.service.service.ClickStreamService;
import com.od.data.service.service.CoverageAnalyticsService;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.MetricUtil;

/**
 * @author Sonali Rawool
 * 
 */
@RestController
public class CoverageAnalyticsController {

  private static final Logger LOGGER = Logger.getLogger(CoverageAnalyticsController.class);

  //private ClickStreamService clickStreamService;
  private CoverageAnalyticsService service;

  @Autowired
  @Qualifier("dummyCoverageAnalyticsService")
  //private ClickStreamService dummyClickStreamService;
  private CoverageAnalyticsService dummyCoverageAnalyticsService;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("initBinder(): registering custom property data binder for List");
    }
  }

  /**
   * Method to return queried records from HBase
   * 
   * @param request - hold CoverageAnalytics Request
   * @return CoverageAnalytics Response with populated rows with data returned from HBase.
   * @throws ResourcesBusyException
   * @throws InterruptedException
   */
  @RequestMapping(value = "/coverageAnalytics/data", method = RequestMethod.GET)
  public Response getRecords(@Valid CoverageAnalyticsRequest request) {
    long arrivalTime = System.currentTimeMillis();
    LOGGER.info("getRecords(): Request recieved " + request);
    Response response = service.getRecordsForCoverageAnalytics(request);
    LOGGER.info("getRecords(): Returning response  time taken(Millis)=" + (System.currentTimeMillis() - arrivalTime));
    return response;
  }

  /**
   * Method to return Phoenix HBase query for the queried GET request.
   * 
   * @param request - hold CoverageAnalytics Request
   * @return CoverageAnalytics response with populated phoenix query field and no rows.
   */
  @RequestMapping(value = "/coverageAnalytics-query/data", method = RequestMethod.GET)
  public Response getPhoenixJdbcQuery(@Valid CoverageAnalyticsRequest request) {
    Response response = service.getJDBCQuery(request);
    return response;
  }

  /**
   * Method to get list of supported dimensions.
   * 
   * @return Collection of supported dimensions.
   */
  @RequestMapping(value = "/coverageAnalytics/dimensions")
  public Collection<Dimension> getSupportedDimensions() {
    return DimensionUtil.getSupportedDimensions(Module.COVERAGEANALYTICS);
  }

  /**
   * Method to get list of supported metrics.
   * 
   * @return Collection of supported metrics.
   */
  @RequestMapping(value = "/coverageAnalytics/metrics")
  public Collection<Metric> getSupportedMetrics() {
    return MetricUtil.getSupportedMetrics(Module.COVERAGEANALYTICS);
  }

  @Autowired 
  @Qualifier("coverageAnalyticsService")
  public void setCoverageAnalyticsService(CoverageAnalyticsService service) {
    this.service = service;
  }

}
