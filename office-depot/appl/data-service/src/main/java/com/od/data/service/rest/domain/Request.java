package com.od.data.service.rest.domain;

import java.util.List;

/**
 * @author sandeep
 * 
 */
public interface Request {

  Module getModule();

  String getFromDate();

  String getToDate();

  String getDimensions();

  String getMetrics();

  List<String> getFilters();

  Integer getLimit();

  OutputFormat getOutput();
}
