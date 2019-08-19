package com.od.data.service.service.impl;

import org.springframework.stereotype.Service;

import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.ClickStreamService;

/**
 * @author sandeep
 * 
 */
@Service("dummyClickStreamService")
public class ClickStreamDummyServiceImpl implements ClickStreamService {

  @Override
  public Response getRecordsForClickStream(ClickStreamRequest clickStreamRequest) {
    // TODO complete this
    return null;
  }

  @Override
  public Response getJDBCQuery(Request clickStreamRequest) {
    // TODO complete this
    return null;
  }

}
