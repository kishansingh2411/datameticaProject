package com.od.data.service.rest.domain;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotBlank;

import com.od.data.service.rest.domain.annotations.Dimension;
import com.od.data.service.rest.domain.annotations.Filters;
import com.od.data.service.rest.domain.annotations.Limit;
import com.od.data.service.rest.domain.annotations.Metric;
import com.od.data.service.rest.domain.annotations.RequestValidation;

/**
 * @author sandeep
 * 
 */
@RequestValidation
public class ClickStreamRequest implements Request {

  @NotBlank(message = "FROM_DATE_NOT_AVAILABLE")
  private String fromDate;

  @NotNull(message = "TO_DATE_NOT_AVAILABLE")
  private String toDate;

  @Dimension(message = "INVALID_DIMENSION", module = Module.CLICKSTREAM)
  private String dimensions;

  @NotBlank(message = "METRICS_NOT_AVAILABLE")
  @Metric(message = "INVALID_METRIC", module = Module.CLICKSTREAM)
  private String metrics;

  @Filters(message = "INVALID_FILTER", module = Module.CLICKSTREAM)
  private List<String> filters;

  @Limit(message = "INVALID_LIMIT")
  private Integer limit;

  private OutputFormat output;

  @Override
  public String getFromDate() {
    return fromDate;
  }

  public void setFromDate(String fromDate) {
    this.fromDate = fromDate;
  }

  @Override
  public String getToDate() {
    return toDate;
  }

  public void setToDate(String toDate) {
    this.toDate = toDate;
  }

  @Override
  public String getDimensions() {
    return dimensions;
  }

  public void setDimensions(String dimensions) {
    this.dimensions = dimensions;
  }

  @Override
  public String getMetrics() {
    return metrics;
  }

  public void setMetrics(String metrics) {
    this.metrics = metrics;
  }

  @Override
  public List<String> getFilters() {
    return filters;
  }

  public void setFilters(List<String> filters) {
    this.filters = filters;
  }

  @Override
  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  @Override
  public OutputFormat getOutput() {
    return output;
  }

  public void setOutput(OutputFormat output) {
    this.output = output;
  }

  @Override
  public Module getModule() {
    return Module.CLICKSTREAM;
  }

  @Override
  public String toString() {
    return "ClickStreamRequest [fromDate=" + fromDate + ", toDate=" + toDate + ", dimensions=" + dimensions + ", metrics=" + metrics
        + ", filters=" + filters + ", limit=" + limit + ", output=" + output + "]";
  }

}
