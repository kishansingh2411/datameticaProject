package com.od.data.service.rest.domain;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.List;

/**
 * @author sandeep
 * 
 */
@XmlRootElement(name = "metrics")
@XmlAccessorType(XmlAccessType.FIELD)
public class Metrics {

  @XmlElement(name = "metric")
  private List<Metric> metrics;

  public List<Metric> getMetrics() {
    return metrics;
  }

  public void setMetrics(List<Metric> metrics) {
    this.metrics = metrics;
  }
}
