package com.od.data.service.service;

import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;

/**
 * @author sandeep
 * 
 */
public interface ClickStreamService {

  /**
   * 
   * @param clickStreamRequest
   * @return
   */
  Response getRecordsForClickStream(ClickStreamRequest clickStreamRequest);

  /**
   * 
   * @param clickStreamRequest
   * @return
   */
  Response getJDBCQuery(Request clickStreamRequest);

}
