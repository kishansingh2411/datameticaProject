package com.od.data.service.rest.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * @author sandeep
 * 
 */
public class Filter {
  private String name;
  private FilterOperator filterOperator;
  private String value;
  private List<Filter> orFilters;

  public Filter() {
  }

  public Filter(String filterString) {
    String[] filters = filterString.trim().split(",");
    int filterCount = filters.length;

    String[] values = filters[0].split(" ");
    this.name = values[0].trim();
    String operationString = values[1].trim();
    this.value = values[2];
    this.filterOperator = FilterOperator.getFilterOperator(operationString);
    if (filterCount > 1) {
      this.orFilters = new ArrayList<Filter>(filterCount - 1);
      for (int i = 1; i < filterCount; i++) {
        this.orFilters.add(new Filter(filters[i]));
      }
    }
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public FilterOperator getFilterOperator() {
    return filterOperator;
  }

  public void setFilterOperator(FilterOperator filterOperator) {
    this.filterOperator = filterOperator;
  }

  public String getValue() {
    return value;
  }

  public void setValue(String value) {
    this.value = value;
  }

  public List<Filter> getOrFilters() {
    return orFilters;
  }

  public void setOrFilters(List<Filter> orFilters) {
    this.orFilters = orFilters;
  }

  public void addOrFilter(Filter orFilter) {
    if (orFilters == null) {
      orFilters = new ArrayList<Filter>(1);
    }
    orFilters.add(orFilter);
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(name).append(" ").append(filterOperator.name()).append(" ").append(value);
    if (null != orFilters && !orFilters.isEmpty()) {
      for (Filter f : orFilters) {
        builder.append(" OR ");
        builder.append(f.toString());
      }
    }
    return builder.toString();
  }
}
