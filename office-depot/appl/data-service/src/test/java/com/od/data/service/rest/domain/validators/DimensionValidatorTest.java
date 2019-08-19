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
import com.od.data.service.rest.domain.annotations.Dimension;
import com.od.data.service.rest.domain.validators.DimensionValidator;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class DimensionValidatorTest {

  private DimensionValidator dimensionValidator;
  private ConstraintValidatorContext context;

  @Before
  public void setup() {
    dimensionValidator = new DimensionValidator();
    dimensionValidator.initialize(new Dimension() {

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
    String dimensionString = null;
    Boolean isValid = dimensionValidator.isValid(dimensionString, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForBlank() {
    String dimensionString = "";
    Boolean isValid = dimensionValidator.isValid(dimensionString, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForEmpty() {
    String dimensionString = " ";
    Boolean isValid = dimensionValidator.isValid(dimensionString, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForInvalidDimensions() {
    String dimensionString = "country,visitorSeg";
    Boolean isValid = dimensionValidator.isValid(dimensionString, context);

    Assert.assertFalse(isValid);
  }

  @Test
  public void testIsValidForValidDimensions() {
    String dimensionString = "country,visitorSegment";
    Boolean isValid = dimensionValidator.isValid(dimensionString, context);

    Assert.assertTrue(isValid);
  }
}
