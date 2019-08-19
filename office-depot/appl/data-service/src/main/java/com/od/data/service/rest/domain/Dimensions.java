package com.od.data.service.rest.domain;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;

/**
 * @author sandeep
 * 
 */
@XmlRootElement(name = "dimensions")
@XmlAccessorType(XmlAccessType.FIELD)
public class Dimensions {

  @XmlElement(name = "dimension")
  private List<Dimension> dimensions;

  public List<Dimension> getDimensions() {
    return dimensions;
  }

  public void setDimensions(List<Dimension> dimensions) {
    this.dimensions = dimensions;
  }
}
