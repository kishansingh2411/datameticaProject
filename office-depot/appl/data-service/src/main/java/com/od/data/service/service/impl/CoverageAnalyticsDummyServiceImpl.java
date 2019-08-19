package com.od.data.service.service.impl;

import org.springframework.stereotype.Service;

//import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.CoverageAnalyticsRequest;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;
//import com.od.data.service.service.ClickStreamService;
import com.od.data.service.service.CoverageAnalyticsService;

/**
 * @author Sonali Rawool
 * 
 */
@Service("dummyCoverageAnalyticsService")
public class CoverageAnalyticsDummyServiceImpl implements CoverageAnalyticsService {

  @Override
  public Response getRecordsForCoverageAnalytics(CoverageAnalyticsRequest request) {
    // TODO complete this
    return null;
  }

  @Override
  public Response getJDBCQuery(Request clickStreamRequest) {
    // TODO complete this
    return null;
  }

}
