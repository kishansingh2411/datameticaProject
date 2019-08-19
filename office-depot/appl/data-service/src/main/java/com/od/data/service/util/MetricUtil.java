/**
 * 
 */
package com.od.data.service.util;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;
import org.springframework.core.io.ClassPathResource;

import com.od.data.service.constants.Constants;
import com.od.data.service.rest.domain.Metric;
import com.od.data.service.rest.domain.Metrics;
import com.od.data.service.rest.domain.Module;

/**
 * @author sandeep
 * 
 */
public class MetricUtil {

  private static final String METRIC_MAPPING_XML_PREFIX = "metricMapping-";
  private static final String METRIC_MAPPING_XML_SUFFIX = ".xml";
  private static final Logger LOGGER = Logger.getLogger(MetricUtil.class);
  private static Map<Module, Map<String, Metric>> moduleMappings;
  private static String environmentPrefix;

  static {
    setup();
  }

  private static void setup() {
    environmentPrefix = ConfigUtil.getProperty(Constants.ENVIORNMENT_CONFIG_PATH_PREFIX_PROPERTY, "");
    try {
      moduleMappings = new HashMap<Module, Map<String, Metric>>();
      prepareModuleMetricMappings(Module.CLICKSTREAM);
      prepareModuleMetricMappings(Module.COVERAGEANALYTICS);
    } catch (JAXBException e) {
      LOGGER.error("Error parsing metrics from xml.", e);
    } catch (IOException e) {
      LOGGER.error("Error reading dimension xml.", e);
    }
  }

  private static void prepareModuleMetricMappings(Module module) throws JAXBException, IOException {
    JAXBContext context = JAXBContext.newInstance(Metrics.class);

    Unmarshaller unmarshaller = context.createUnmarshaller();
    Metrics metrics = (Metrics) unmarshaller.unmarshal(new ClassPathResource(environmentPrefix + METRIC_MAPPING_XML_PREFIX
        + module.getDisplayName() + METRIC_MAPPING_XML_SUFFIX).getFile());
    Map<String, Metric> supportedMetrics = new HashMap<String, Metric>();
    for (Metric metric : metrics.getMetrics()) {
      supportedMetrics.put(metric.getName(), metric);
    }
    moduleMappings.put(module, supportedMetrics);
  }

  public static Boolean isSupported(Module module, String metricString) {
    return moduleMappings.get(module).get(metricString) != null ? Boolean.TRUE : Boolean.FALSE;
  }

  public static Metric getMetric(Module module, String metricName) {
    return moduleMappings.get(module).get(metricName);
  }

  public static Collection<Metric> getSupportedMetrics(Module module) {
    return new LinkedList<Metric>(moduleMappings.get(module).values());
  }
}
