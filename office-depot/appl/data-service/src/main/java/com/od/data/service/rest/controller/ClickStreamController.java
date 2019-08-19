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

import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Metric;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.ClickStreamService;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.MetricUtil;

/**
 * @author sandeep
 * 
 */
@RestController
public class ClickStreamController {

  private static final Logger LOGGER = Logger.getLogger(ClickStreamController.class);

  private ClickStreamService clickStreamService;

  @Autowired
  @Qualifier("dummyClickStreamService")
  private ClickStreamService dummyClickStreamService;

  @InitBinder
  public void initBinder(WebDataBinder binder) {
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("initBinder(): registering custom property data binder for List");
    }
  }

  /**
   * Method to return queried records from HBase
   * 
   * @param clickStreamRequest
   * @return ClickStream Response with populated rows with data returned from
   *         HBase.
   * @throws ResourcesBusyException
   * @throws InterruptedException
   */
  @RequestMapping(value = "/clickstream/data", method = RequestMethod.GET)
  public Response getRecords(@Valid ClickStreamRequest clickStreamRequest) {
    long arrivalTime = System.currentTimeMillis();
    LOGGER.info("getRecords(): Request recieved " + clickStreamRequest);
    Response clickStreamResponse = clickStreamService.getRecordsForClickStream(clickStreamRequest);
    LOGGER.info("getRecords(): Returning response  time taken(Millis)=" + (System.currentTimeMillis() - arrivalTime));
    return clickStreamResponse;
  }

  /**
   * Method to return Phoenix HBase query for the queried GET request.
   * 
   * @param clickStreamRequest
   * @return ClickStream response with populated phoenix query field and no
   *         rows.
   */
  @RequestMapping(value = "/clickstream-query/data", method = RequestMethod.GET)
  public Response getPhoenixJdbcQuery(@Valid ClickStreamRequest clickStreamRequest) {
    Response clickStreamResponse = clickStreamService.getJDBCQuery(clickStreamRequest);
    return clickStreamResponse;
  }

  /**
   * Method to get list of supported dimensions.
   * 
   * @return Collection of supported dimensions.
   */
  @RequestMapping(value = "/clickstream/dimensions")
  public Collection<Dimension> getSupportedDimensions() {
    return DimensionUtil.getSupportedDimensions(Module.CLICKSTREAM);
  }

  /**
   * Method to get list of supported metrics.
   * 
   * @return Collection of supported metrics.
   */
  @RequestMapping(value = "/clickstream/metrics")
  public Collection<Metric> getSupportedMetrics() {
    return MetricUtil.getSupportedMetrics(Module.CLICKSTREAM);
  }

  @Autowired
  @Qualifier("clickStreamService")
  public void setClickStreamService(ClickStreamService clickStreamService) {
    this.clickStreamService = clickStreamService;
  }

}
