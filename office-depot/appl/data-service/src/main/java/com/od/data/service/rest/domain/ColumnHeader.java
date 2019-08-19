package com.od.data.service.rest.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author sandeep
 * 
 */
public class ColumnHeader {
  @JsonProperty("name")
  private String columnName;
  @JsonProperty("type")
  private ColumnType columnType;
  @JsonProperty("dataType")
  private DataType dataType;
  @JsonIgnore
  private DataType columnDataType;

  public ColumnHeader(String name, ColumnType type, DataType dataType, DataType columnDataType) {
    this.columnName = name;
    this.columnType = type;
    this.dataType = dataType;
    this.columnDataType = columnDataType;
  }
  
  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public ColumnType getColumnType() {
    return columnType;
  }

  public void setColumnType(ColumnType columnType) {
    this.columnType = columnType;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }
  
  public DataType getColumnDataType() {
    return columnDataType;
  }

  public void setColumnDataType(DataType columnDataType) {
    this.columnDataType = columnDataType;
  }
}
