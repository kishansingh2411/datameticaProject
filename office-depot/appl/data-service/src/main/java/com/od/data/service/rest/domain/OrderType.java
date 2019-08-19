package com.od.data.service.rest.domain;

/**
 * @author sandeep
 *
 */
public enum OrderType {
  ASC("asc"), DESC("desc nulls last");
  
  private String ordering;
  
  private OrderType(String ordering) {
    this.ordering = ordering;
  }
  
  public String getOrdering() {
    return ordering;
  }
}
