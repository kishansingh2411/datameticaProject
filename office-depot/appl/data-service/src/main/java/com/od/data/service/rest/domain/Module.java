package com.od.data.service.rest.domain;

/**
 * @author sandeep
 *
 */
public enum Module {
  CLICKSTREAM("clickstream"),
  COVERAGEANALYTICS("coverageAnalytics");

  private String displayName;
  private String keyDimensionName;

  private Module(String displayName) {
    this.displayName = displayName;
  }

  public String getDisplayName() {
    return displayName;
  }

  public String getKeyDimensionName() {
    return keyDimensionName;
  }
}
