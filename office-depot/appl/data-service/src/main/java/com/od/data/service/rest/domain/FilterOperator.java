package com.od.data.service.rest.domain;


/**
 * @author sandeep
 * 
 */
public enum FilterOperator {
  EQ("=", Boolean.TRUE, 0.001, Double.POSITIVE_INFINITY), 
  NOTEQ("<>", Boolean.TRUE, 0, 0), 
  GT(">", Boolean.TRUE, 0, Double.POSITIVE_INFINITY), 
  GTEQ(">=", Boolean.TRUE, 0.001, Double.POSITIVE_INFINITY), 
  LT("<", Boolean.TRUE, Double.NEGATIVE_INFINITY, 0), 
  LTEQ("<=", Boolean.TRUE, Double.NEGATIVE_INFINITY, -0.001);

  private String databaseOperator;
  private Boolean nullCheckRequired;
  private Double minNullCheckValue;
  private Double maxNullCheckValue;
  
  FilterOperator(String databaseOperator, Boolean nullCheckRequired, double minNullCheckValue, double maxNullCheckValue) {
    this.databaseOperator = databaseOperator;
    this.nullCheckRequired = nullCheckRequired;
    this.minNullCheckValue = minNullCheckValue;
    this.maxNullCheckValue = maxNullCheckValue;
  }

  public static FilterOperator getFilterOperator(String operatorString) {
    FilterOperator filterOperator = null;
    for (FilterOperator operator : FilterOperator.values()) {
      if (operator.name().equalsIgnoreCase(operatorString)) {
        filterOperator = operator;
        break;
      }
    }

    return filterOperator;
  }

  public String getDatabaseOperator() {
    return databaseOperator;
  }
  
  public Boolean getNullCheckRequired() {
    return nullCheckRequired;
  }
  
  public Double getMinNullCheckValue() {
    return minNullCheckValue;
  }
  
  public Double getMaxNullCheckValue() {
    return maxNullCheckValue;
  }
}
