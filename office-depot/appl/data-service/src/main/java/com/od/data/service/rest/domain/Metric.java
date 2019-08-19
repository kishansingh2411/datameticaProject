package com.od.data.service.rest.domain;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * @author sandeep
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Metric {

  @XmlElement(name = "name")
  private String name;
  @XmlElement(name = "dataType")
  private DataType dataType;
  @XmlElement(name = "columnName")
  private String columnName;
  @XmlElement(name = "columnDataType")
  private DataType columnDataType;
  @XmlElement(name = "derived")
  private Boolean derived;

  public Boolean getDerived() {
    return derived;
  }

  public void setDerived(Boolean derived) {
    this.derived = derived;
  }

  public Metric() {
  }

  public Metric(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataType getDataType() {
    return dataType;
  }

  public void setDataType(DataType dataType) {
    this.dataType = dataType;
  }

  public String getColumnName() {
    return columnName;
  }

  public void setColumnName(String columnName) {
    this.columnName = columnName;
  }

  public DataType getColumnDataType() {
    return columnDataType;
  }

  public void setColumnDataType(DataType columnDataType) {
    this.columnDataType = columnDataType;
  }

  @Override
  public String toString() {
    return "Metric [name=" + name + ", dataType=" + dataType + ", columnName=" + columnName + ", columnDataType=" + columnDataType
        + ", derived=" + derived + "]";
  }
}
