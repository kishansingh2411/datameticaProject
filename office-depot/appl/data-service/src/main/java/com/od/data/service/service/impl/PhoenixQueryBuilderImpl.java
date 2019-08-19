package com.od.data.service.service.impl;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Service;

import com.od.data.service.constants.Constants;
import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.DataType;
import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Filter;
import com.od.data.service.rest.domain.FilterOperator;
import com.od.data.service.rest.domain.Metric;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.Query;
import com.od.data.service.service.QueryBuilder;
import com.od.data.service.util.ConfigUtil;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.MetricUtil;

/**
 * @author sandeep
 * 
 */
@Service
public class PhoenixQueryBuilderImpl implements QueryBuilder {

  @Override
  public String buildQuery(Query query) {
    StringBuilder builder = new StringBuilder("select ");
    addDimensions(builder, query);
    addMetrics(builder, query);
    addFromClause(builder, query);
    addWhereClause(builder, query);
    addLimitClause(builder, query);

    return builder.toString();
  }

  private void addDimensions(StringBuilder builder, Query query) {
    List<Dimension> dimensions = query.getDimensions();
    if (dimensions != null && !dimensions.isEmpty()) {
      int count = 0;
      for (Dimension dimension : dimensions) {
        builder.append(dimension.getColumnName());
        count++;
        if (count < dimensions.size()) {
          builder.append(",");
        }
      }
    }
  }

  private void addMetrics(StringBuilder builder, Query query) {
    int count = 0;
    List<Metric> metrics = query.getMetrics();
    /**
     * There are cases where no dimension will be provided. For example while
     * querying metrics representing business indicators no specific dimension
     * is provided and only metrics. In such case append , prior to metrics
     * column only when dimension is provided
     */
    if (query.getDimensions() != null && !query.getDimensions().isEmpty()) {
      builder.append(",");
    }
    for (Metric metric : metrics) {
      Boolean wrap = !metric.getDerived();
      builder.append(getWrappedMetricColumn(metric, wrap));
      count++;
      if (count < metrics.size()) {
        builder.append(",");
      }
    }
    builder.append(" ");
  }

  private void addFromClause(StringBuilder builder, Query query) {
    builder.append("from ").append(getTableName(query.getRequest().getModule(), query.getAggregatorLevel())).append(" ");
  }

  private String getTableName(Module module, AggregatorLevel aggregatorLevel) {
    StringBuilder builder = new StringBuilder();
    builder.append(Constants.PROPERTY_MODULE_PREFIX).append(Constants.PROPERTY_SEPERATOR).append(module.getDisplayName())
        .append(Constants.PROPERTY_SEPERATOR).append(Constants.TABLE_NAME_PROPERTY_AGGREGATOR_PREFIX).append(Constants.PROPERTY_SEPERATOR)
        .append(aggregatorLevel.getDisplayName()).append(Constants.PROPERTY_SEPERATOR).append(Constants.TABLE_NAME_PROPERTY_SUFFIX);

    return ConfigUtil.getProperty(builder.toString());
  }

  private void addWhereClause(StringBuilder builder, Query query) {
    Module module = query.getRequest().getModule();

    builder.append("where ");
    addDateRange(builder, query);
    if (null != query.getDimensionFilters() && !query.getDimensionFilters().isEmpty()) {
      builder.append(" and ");
    }
    addDimensionFilters(builder, query.getDimensionFilters(), module, "and ");
    addDimensionValueKnobs(builder, query, module, "and");

    List<Dimension> dimensions = query.getDimensions();
    if (dimensions == null) {
      dimensions = new LinkedList<Dimension>();
    }

    addGroupByHaving(builder, query);

    addMetricFilters(builder, query.getMetricFilters(), " and ", module);
  }

  private void addDimensionValueKnobs(StringBuilder builder, Query query, Module module, String filterSeparator) {
    Map<Dimension, String> dimensionKnobValueMap = new HashMap<>();
    if (null != query.getDimensions() && !query.getDimensions().isEmpty()) {
      for (Dimension currentDimension : query.getDimensions()) {
        // check whether current dimension was in filter list or not, add knob
        // only when no filter was provided for dimension
        if (!isDimensionFilterApplied(query.getDimensionFilters(), currentDimension, module)) {
          String knobValues = DimensionUtil.getKnobValues(module, currentDimension);
          if (!StringUtils.isEmpty(knobValues)) {
            dimensionKnobValueMap.put(currentDimension, knobValues);
          }
        }
      }
      if (!dimensionKnobValueMap.isEmpty()) {
        builder.append(" and ");
        int count = 0;
        for (Map.Entry<Dimension, String> entry : dimensionKnobValueMap.entrySet()) {
          builder.append(entry.getKey().getColumnName()).append(" ").append(entry.getKey().getKnobValues()).append(" ");
          count++;
          if (count < dimensionKnobValueMap.size()) {
            builder.append(" and ");
          }
        }
      }
    }
  }

  private boolean isDimensionFilterApplied(List<Filter> filters, Dimension currentDimension, Module module) {
    boolean returnVal = false;
    for (Filter dimensionFilter : filters) {
      Dimension filterDimension = DimensionUtil.getDimension(module, dimensionFilter.getName());
      if (filterDimension == currentDimension) {
        returnVal = true;
        break;
      }
    }
    return returnVal;
  }

  private void addDimensionFilters(StringBuilder builder, List<Filter> dimensionFilters, Module module, String filterSeparator) {
    int count = 0;

    for (Filter dimensionFilter : dimensionFilters) {
      Dimension filterDimension = DimensionUtil.getDimension(module, dimensionFilter.getName());
      List<Filter> orFilters = dimensionFilter.getOrFilters();
      if (null != orFilters && !orFilters.isEmpty()) {
        builder.append("(");
      }
      builder.append(filterDimension.getColumnName());
      builder.append(dimensionFilter.getFilterOperator().getDatabaseOperator())
          .append(getWrappedDimensionValue(filterDimension, dimensionFilter.getValue())).append(" ");
      if (null != orFilters && !orFilters.isEmpty()) {
        builder.append("or ");
        addDimensionFilters(builder, orFilters, module, "or ");
        builder.append(") ");
      }
      count++;
      if (count < dimensionFilters.size()) {
        builder.append(filterSeparator);
      }
    }
  }

  private void addGroupByHaving(StringBuilder builder, Query query) {
    if (query.getDimensions() != null && !query.getDimensions().isEmpty()) {
      builder.append(" group by ");
    }
    addDimensions(builder, query);
    builder.append(" ");
    if (query.getMetricFilters() != null && !query.getMetricFilters().isEmpty()) {
      builder.append("having ");
    }
  }

  private void addLimitClause(StringBuilder builder, Query query) {
    if (null != query.getLimit() && query.getLimit() > 0) {
      builder.append("limit ").append(query.getLimit());
    }
  }

  private void addDateRange(StringBuilder builder, Query query) {
    String dateColumnProperty = query.getAggregatorLevel().getDisplayName() + Constants.PROPERTY_SEPERATOR
        + Constants.DATE_DIMENSION_PROPERTY;
    Dimension dateDimension = DimensionUtil.getDimension(query.getRequest().getModule(), ConfigUtil.getProperty(dateColumnProperty));
    builder.append(dateDimension.getColumnName()).append(">=").append(getWrappedDimensionValue(dateDimension, query.getFromDate()))
        .append(" and ").append(dateDimension.getColumnName()).append("<=")
        .append(getWrappedDimensionValue(dateDimension, query.getToDate()));
  }

  private void addMetricFilters(StringBuilder builder, List<Filter> metricFilters, String filterSeparator, Module module) {
    int count = 0;
    for (Filter metricFilter : metricFilters) {
      Metric filterMetric = MetricUtil.getMetric(module, metricFilter.getName());
      List<Filter> orFilters = metricFilter.getOrFilters();
      if (null != orFilters && !orFilters.isEmpty()) {
        builder.append("(");
      }

      Double filterValue = Double.valueOf(metricFilter.getValue());
      FilterOperator filterOperator = metricFilter.getFilterOperator();
      Boolean wrap = !filterMetric.getDerived();
      Boolean nullCheckRequired = filterOperator.getNullCheckRequired() && filterValue >= filterOperator.getMinNullCheckValue()
          && filterValue <= filterOperator.getMaxNullCheckValue();

      if (nullCheckRequired) {
        builder.append("(");
      }
      builder.append(getWrappedMetricColumn(filterMetric, wrap)).append(filterOperator.getDatabaseOperator())
          .append(getWrappedValue(metricFilter.getValue(), filterMetric.getColumnDataType()));

      if (nullCheckRequired) {
        builder.append(" and ").append(getWrappedMetricColumn(filterMetric, wrap)).append(" is not null) ");

      }

      if (null != orFilters && !orFilters.isEmpty()) {
        builder.append(" or ");
        addMetricFilters(builder, orFilters, " or ", module);
        builder.append(") ");
      }

      count++;
      if (count < metricFilters.size()) {
        builder.append(filterSeparator);
      }
    }
    if (count > 0) {
      builder.append(" ");
    }
  }

  private String getWrappedDimensionValue(Dimension dimension, String value) {
    return getWrappedValue(value, dimension.getColumnDataType());
  }

  private String getWrappedValue(String value, DataType dataType) {
    if (dataType.getNeedQuotes()) {
      value = "\'" + value + "\'";
    }
    return value;
  }

  private String getWrappedMetricColumn(Metric metric, Boolean wrap) {
    String metricColumn = metric.getColumnName();
    if (wrap) {
      metricColumn = "sum(" + metricColumn + ")";
    }
    return metricColumn;
  }
}
