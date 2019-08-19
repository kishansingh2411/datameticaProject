package com.od.data.service.util;

import java.util.Collection;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.util.DimensionUtil;

/**
 * @author sandeep
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class DimensionUtilTest {

  @Test
  public void testDimensions() {
    testModuleSpecificDimensions(Module.CLICKSTREAM);
    testModuleSpecificDimensions(Module.COVERAGEANALYTICS);
  }

  private void testModuleSpecificDimensions(Module module) {
    Collection<Dimension> supportedDimensions = DimensionUtil.getSupportedDimensions(module);

    Assert.assertNotNull(supportedDimensions);
    Assert.assertTrue("Number of supported dimensions, actual 0, expected >0.", supportedDimensions.size() > 0);

    for (Dimension expectedDim : supportedDimensions) {
      Dimension actualDimension = DimensionUtil.getDimension(module, expectedDim.getName());

      Assert.assertNotNull(actualDimension);
      Assert.assertEquals(expectedDim, actualDimension);

    }
  }
}
