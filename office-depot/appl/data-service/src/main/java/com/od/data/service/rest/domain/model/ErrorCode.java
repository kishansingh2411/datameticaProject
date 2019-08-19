package com.od.data.service.rest.domain.model;

/**
 * @author sandeep
 * 
 */
public enum ErrorCode {
  INVALID_DIMENSION("error.dimension.invalid"), INVALID_DIMENSION_COUNT("error.dimension.count.invalid"), METRICS_NOT_AVAILABLE(
      "error.metric.not.available"), FROM_DATE_NOT_AVAILABLE("error.fromDate.not.available"), TO_DATE_NOT_AVAILABLE(
      "error.toDate.not.available"), INVALID_METRIC("error.metric.invalid"), INVALID_METRIC_COUNT("error.metric.count.invalid"), INVALID_DATE_FORMAT(
      "error.date.format.invalid"), INVALID_LIMIT("error.limit.invalid"), INVALID_FILTER("error.filter.invalid"), INTERNAL_SERVER_ERROR(
      "error.internal.server.error");

  private ErrorCode(String description) {
    this.description = description;
  }

  private String description;

  public String description() {
    return description;
  }
}
