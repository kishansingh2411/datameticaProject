package com.od.data.service.service;

import com.od.data.service.rest.domain.CoverageAnalyticsRequest;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;

/**
 * @author Sonali Rawool
 * 
 */
public interface CoverageAnalyticsService {

  /**
   * 
   * @param request
   * @return
   */
  Response getRecordsForCoverageAnalytics(CoverageAnalyticsRequest request);

  /**
   * 
   * @param request
   * @return
   */
  Response getJDBCQuery(Request request);

}
