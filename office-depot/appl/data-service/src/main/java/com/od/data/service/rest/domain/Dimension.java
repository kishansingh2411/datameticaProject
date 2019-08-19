package com.od.data.service.rest.domain;

import java.lang.annotation.Annotation;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;

/**
 * @author sandeep
 * 
 */
@XmlAccessorType(XmlAccessType.FIELD)
public class Dimension implements Annotation {

  @XmlElement(name = "name")
  private String name;
  @XmlElement(name = "dataType")
  private DataType datatype;
  @XmlElement(name = "columnName")
  private String columnName;
  @XmlElement(name = "columnDataType")
  private DataType columnDataType;
  @XmlElement(name = "derived")
  private Boolean derived;
  @XmlElement(name = "knobValues")
  private String knobValues;

  public Dimension() {
  }

  public Dimension(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public DataType getDatatype() {
    return datatype;
  }

  public void setDatatype(DataType datatype) {
    this.datatype = datatype;
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

  public Boolean getDerived() {
    return derived;
  }

  public void setDerived(Boolean derived) {
    this.derived = derived;
  }

  public String getKnobValues() {
    return knobValues;
  }

  public void setKnobValues(String knobValues) {
    this.knobValues = knobValues;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((name == null) ? 0 : name.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Dimension other = (Dimension) obj;
    if (name == null) {
      if (other.name != null)
        return false;
    } else if (!name.equals(other.name))
      return false;
    return true;
  }

  @Override
  public Class<? extends Annotation> annotationType() {
    return null;
  }

  @Override
  public String toString() {
    return "Dimension [name=" + name + ", datatype=" + datatype + ", columnName=" + columnName + ", columnDataType=" + columnDataType
        + ", derived=" + derived + ", knobValues=" + knobValues + "]";
  }

}
