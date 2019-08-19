package com.od.data.service.rest.domain.validators;

import java.lang.annotation.Annotation;

import javax.validation.ConstraintValidatorContext;
import javax.validation.Payload;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.annotations.Metric;
import com.od.data.service.rest.domain.validators.MetricValidator;

/**
 * @author sandeep
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class MetricValidatorTest {

  private MetricValidator metricValidator;

  private ConstraintValidatorContext context;

  @Before
  public void setup() {
    metricValidator = new MetricValidator();
    metricValidator.initialize(new Metric() {

      @Override
      public Class<? extends Annotation> annotationType() {
        return null;
      }

      @Override
      public Class<? extends Payload>[] payload() {
        return null;
      }

      @Override
      public Module module() {
        return Module.CLICKSTREAM;
      }

      @Override
      public String message() {
        return null;
      }

      @Override
      public Class<?>[] groups() {
        return null;
      }

    });

    context = Mockito.mock(ConstraintValidatorContext.class);
  }

  @Test
  public void testIsValidForNull() {
    String metricString = null;
    Boolean isValid = metricValidator.isValid(metricString, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForEmpty() {
    String metricString = "";
    Boolean isValid = metricValidator.isValid(metricString, context);
    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForBlank() {
    String metricString = " ";
    Boolean isValid = metricValidator.isValid(metricString, context);
    Assert.assertTrue(isValid);
  }

  @Test
  public void testInvalidMetric() {
    String metricString = "abc,def,ghi,llll";
    Boolean isValid = metricValidator.isValid(metricString, context);
    Assert.assertFalse(isValid);
  }

  @Test
  public void testValidMetric() {
    String metricString = "totalOrders,totalRevenue";
    Boolean isValid = metricValidator.isValid(metricString, context);
    Assert.assertTrue(isValid);
  }
}
