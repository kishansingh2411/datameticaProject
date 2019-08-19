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

import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Dimensions;
import com.od.data.service.rest.domain.Module;

/**
 * @author sandeep
 * 
 */
public class DimensionUtil {

  private static final String DIMENSION_MAPPING_XML_PREFIX = "dimensionMapping-";
  private static final String DIMENSION_MAPPING_XML_SUFFIX = ".xml";
  private static final Logger LOGGER = Logger.getLogger(DimensionUtil.class);
  public static Map<Module, Map<Mapping, Map<String, Dimension>>> moduleMapping;

  private enum Mapping {
    SUPPORTED_DIMENSIONS;
  }

  static {
    setup();
  }

  private static void setup() {
    try {
      moduleMapping = new HashMap<>();
      prepareModuleDimensionMappings(Module.CLICKSTREAM);
      prepareModuleDimensionMappings(Module.COVERAGEANALYTICS);
    } catch (JAXBException e) {
      LOGGER.error("Error parsing dimensions from xml.", e);
    } catch (IOException e) {
      LOGGER.error("Error reading dimension xml.", e);
    }
  }

  private static void prepareModuleDimensionMappings(Module module) throws JAXBException, IOException {
    Map<String, Dimension> supportedDimensions = new HashMap<String, Dimension>();

    JAXBContext context = JAXBContext.newInstance(Dimensions.class);
    Unmarshaller unmarshaller = context.createUnmarshaller();
    Dimensions dimensions = (Dimensions) unmarshaller.unmarshal(new ClassPathResource(DIMENSION_MAPPING_XML_PREFIX
        + module.getDisplayName() + DIMENSION_MAPPING_XML_SUFFIX).getFile());

    for (Dimension dimension : dimensions.getDimensions()) {
      supportedDimensions.put(dimension.getName(), dimension);
    }

    Map<Mapping, Map<String, Dimension>> mappingDimensions = new HashMap<Mapping, Map<String, Dimension>>();
    mappingDimensions.put(Mapping.SUPPORTED_DIMENSIONS, supportedDimensions);

    moduleMapping.put(module, mappingDimensions);
  }

  public static Boolean isSupported(Module module, String dimensionString) {
    Dimension dimension = moduleMapping.get(module).get(Mapping.SUPPORTED_DIMENSIONS).get(dimensionString);
    return dimension != null ? Boolean.TRUE : Boolean.FALSE;
  }

  public static Dimension getDimension(Module module, String dimensionName) {
    return moduleMapping.get(module).get(Mapping.SUPPORTED_DIMENSIONS).get(dimensionName);
  }

  public static Collection<Dimension> getSupportedDimensions(Module module) {
    return new LinkedList<Dimension>(moduleMapping.get(module).get(Mapping.SUPPORTED_DIMENSIONS).values());
  }

  public static String getKnobValues(Module module, Dimension dimension) {
    return ((Dimension) moduleMapping.get(module).get(Mapping.SUPPORTED_DIMENSIONS).get(dimension.getName())).getKnobValues();
  }
}
