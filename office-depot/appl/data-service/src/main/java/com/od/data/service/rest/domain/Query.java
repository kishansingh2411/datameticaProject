package com.od.data.service.rest.domain;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.MetricUtil;
import com.od.data.service.util.QuarterDateFormat;

/**
 * @author sandeep
 * 
 */
@JsonPropertyOrder({"dimensions", "metrics", "filters", "fromDate", "toDate", "limit"})
public class Query {

  private static final Logger LOGGER = Logger.getLogger(Query.class);

  @JsonIgnore
  private Request request;
  @JsonIgnore
  private List<Dimension> dimensions;
  @JsonIgnore
  private List<Metric> metrics;
  @JsonIgnore
  private List<Filter> dimensionFilters;
  @JsonIgnore
  private List<Filter> metricFilters;
  @JsonIgnore
  private String fromDate;
  @JsonIgnore
  private String toDate;
  @JsonInclude(Include.NON_NULL)
  private Integer limit;
  @JsonIgnore
  private AggregatorLevel aggregatorLevel;
  @JsonIgnore
  private String appliedDatePattern;

  public Query(Request request, Boolean isValid) {
    LOGGER.info("Query(): called for (module)=(" + request.getModule().name() + ")");
    this.request = request;
    if (isValid) {
      this.appliedDatePattern = extractAppliedPattern(request.getFromDate(), request.getToDate());
      LOGGER.info("Applied date pattern: " + this.appliedDatePattern);
      this.dimensions = getDimensions(request.getDimensions());
      this.metrics = getMetrics(request.getMetrics());
      parseFilters(request.getFilters());
      this.limit = request.getLimit();
      // if (this.limit == null || this.limit.intValue() <= 0) {
      // this.limit = 1000;
      // }
      this.aggregatorLevel = getAggregateLevel(this.dimensions);
      LOGGER.info("Aggregator Level: " + this.aggregatorLevel);
      this.fromDate = getPhoenixQueryFormattedDate(request.getFromDate());
      this.toDate = getPhoenixQueryFormattedDate(request.getToDate());
    }
  }

  private AggregatorLevel getAggregateLevel(List<Dimension> dimensions) {
    Dimension dateDimension = null;
    AggregatorLevel aggregatorLevel = null;
    if (dimensions != null) {
      for (Dimension dimension : dimensions) {
        if (DataType.DATE == dimension.getDatatype()) {
          dateDimension = dimension;
          break;
        }
      }
    }
    if (dateDimension != null) {
      aggregatorLevel = AggregatorLevel.getAggregateLevel(dateDimension.getName());
    } else {
      Map<String, List<AggregatorLevel>> patternToAggregatorLevelsMap = AggregatorLevel.getPatternToAggregatorLevelsMap(true);
      List<AggregatorLevel> applicableAggregatorLevels = patternToAggregatorLevelsMap.get(appliedDatePattern);
      aggregatorLevel = applicableAggregatorLevels.get(0);
    }
    return aggregatorLevel;
  }

  public List<Dimension> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<Dimension> dimensions) {
    this.dimensions = dimensions;
  }

  public List<Metric> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<Metric> metrics) {
    this.metrics = metrics;
  }

  public List<Filter> getDimensionFilters() {
    return dimensionFilters;
  }

  public void setDimensionFilters(List<Filter> dimensionFilters) {
    this.dimensionFilters = dimensionFilters;
  }

  public List<Filter> getMetricFilters() {
    return metricFilters;
  }

  public void setMetricFilters(List<Filter> metricFilters) {
    this.metricFilters = metricFilters;
  }

  public String getFromDate() {
    return fromDate;
  }

  public void setFromDate(String fromDate) {
    this.fromDate = fromDate;
  }

  public String getToDate() {
    return toDate;
  }

  public void setToDate(String toDate) {
    this.toDate = toDate;
  }

  public Integer getLimit() {
    return limit;
  }

  public void setLimit(Integer limit) {
    this.limit = limit;
  }

  public String getAppliedDatePattern() {
    return appliedDatePattern;
  }

  public static List<ColumnHeader> createColumnHeaders(Query queryObject) {
    return createColumnHeaders(queryObject, false);
  }

  public static List<ColumnHeader> createColumnHeaders(Query queryObject, boolean useAlias) {
    List<ColumnHeader> columnHeaderList = new LinkedList<ColumnHeader>();
    if (queryObject.getDimensions() != null && !queryObject.getDimensions().isEmpty()) {
      for (Dimension dimension : queryObject.getDimensions()) {
        String dimensionName = dimension.getName();
        ColumnHeader columnHeader = new ColumnHeader(dimensionName, ColumnType.DIMENSION, dimension.getDatatype(),
            dimension.getColumnDataType());
        columnHeaderList.add(columnHeader);
      }
    }
    if (queryObject.getMetrics() != null && !queryObject.getMetrics().isEmpty()) {
      for (Metric metric : queryObject.getMetrics()) {
        ColumnHeader columnHeader = new ColumnHeader(metric.getName(), ColumnType.METRIC, metric.getDataType(), metric.getColumnDataType());
        columnHeaderList.add(columnHeader);
      }
    }
    return columnHeaderList;
  }

  @JsonProperty("dimensions")
  @JsonInclude(Include.NON_EMPTY)
  public String getDimensionsString() {
    return request.getDimensions();
  }

  @JsonProperty("metrics")
  public String getMetricsString() {
    return request.getMetrics();
  }

  @JsonInclude(Include.NON_EMPTY)
  @JsonProperty("filters")
  public List<String> getFilters() {
    return request.getFilters();
  }

  @JsonProperty("fromDate")
  @JsonInclude(Include.NON_NULL)
  public String formattedFromDate() {
    return request.getFromDate();
  }

  @JsonProperty("toDate")
  @JsonInclude(Include.NON_NULL)
  public String formattedToDate() {
    return request.getToDate();
  }

  private String getPhoenixQueryFormattedDate(String dateString) {
    String formattedDate = null;
    for (String requestFormat : aggregatorLevel.getValidRequestParams()) {
      DateFormat displayDateFormat;
      DateFormat queryDateFormat;
      if (AggregatorLevel.QUARTER == aggregatorLevel) {
        displayDateFormat = new QuarterDateFormat(requestFormat);
        queryDateFormat = new QuarterDateFormat(aggregatorLevel.getQueryColumnFormat());
      } else {
        displayDateFormat = new SimpleDateFormat(requestFormat);
        queryDateFormat = new SimpleDateFormat(aggregatorLevel.getQueryColumnFormat());
      }
      displayDateFormat.setLenient(Boolean.FALSE);
      try {
        Date requestDate = displayDateFormat.parse(dateString);
        formattedDate = queryDateFormat.format(requestDate);
        break;
      } catch (ParseException e) { // Skipping in case of exception.
      }
    }
    return formattedDate;
  }

  private List<Dimension> getDimensions(String dimensions) {
    List<Dimension> dimensionList = null;
    if (dimensions != null && dimensions.trim().length() > 0) {
      String[] dimensionArray = getValueArray(dimensions);
      dimensionList = new ArrayList<>(dimensionArray.length);
      for (String dimensionString : dimensionArray) {
        dimensionList.add(DimensionUtil.getDimension(request.getModule(), dimensionString));
      }
    }
    return dimensionList;
  }

  private List<Metric> getMetrics(String metrics) {
    String[] metricArray = getValueArray(metrics);
    List<Metric> metricList = new ArrayList<Metric>(metricArray.length);
    for (String metricName : metricArray) {
      metricList.add(MetricUtil.getMetric(request.getModule(), metricName));
    }
    return metricList;
  }

  private String[] getValueArray(String values) {
    String[] valueArray = values.split(",");
    return valueArray;
  }

  private void parseFilters(List<String> filters) {
    dimensionFilters = new LinkedList<Filter>();
    metricFilters = new LinkedList<Filter>();
    if (filters != null && !filters.isEmpty()) {
      for (String filterString : filters) {
        Filter filter = new Filter(filterString);
        if (DimensionUtil.isSupported(request.getModule(), filter.getName())) {
          dimensionFilters.add(filter);
        } else {
          metricFilters.add(filter);
        }
      }
    }
  }

  public Request getRequest() {
    return request;
  }

  public AggregatorLevel getAggregatorLevel() {
    return aggregatorLevel;
  }

  private String extractAppliedPattern(String fromDate, String toDate) {
    AggregatorLevel[] aggregatorLevels = AggregatorLevel.values();
    for (AggregatorLevel level : aggregatorLevels) {
      String pattern = level.getPrimaryDatePattern();
      try {
        DateFormat df;
        if (AggregatorLevel.QUARTER == level) {
          df = new QuarterDateFormat(pattern);
        } else {
          df = new SimpleDateFormat(pattern);
        }
        df.parse(toDate);
        return pattern;
      } catch (Exception e) {
        // doNothing
      }
    }

    return null;
  }

  @Override
  public String toString() {
    return "Query [request=" + request + ", dimensions=" + dimensions + ", metrics=" + metrics + ", dimensionFilters=" + dimensionFilters
        + ", metricFilters=" + metricFilters + ", fromDate=" + fromDate + ", toDate=" + toDate + ", limit=" + limit + ", aggregatorLevel="
        + aggregatorLevel + ", appliedDatePattern=" + appliedDatePattern + "]";
  }
}
