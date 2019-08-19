package com.od.data.service.rest.domain.validators;

import java.lang.annotation.Annotation;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

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
import com.od.data.service.rest.domain.annotations.Filters;
import com.od.data.service.rest.domain.validators.FiltersValidator;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class FiltersValidatorTest {

  private FiltersValidator filtersValidator;
  private ConstraintValidatorContext context;

  @Before
  public void setup() {
    filtersValidator = new FiltersValidator();
    filtersValidator.initialize(new Filters() {

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
    List<String> filters = null;
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForEmpty() {
    List<String> filters = Collections.emptyList();
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertTrue(isValid);
  }

  @Test
  public void testInvalidFormattedFilter() {
    List<String> filters = new LinkedList<String>();
    filters.add("countryEQindia");
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertFalse(isValid);
  }

  @Test
  public void testInvalidFilterDimension() {
    List<String> filters = new LinkedList<String>();
    filters.add("cntry EQ india");
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertFalse(isValid);
  }

  @Test
  public void testUnsupportedFilterMetric() {
    List<String> filters = new LinkedList<String>();
    filters.add("unsupportedMetricFilter EQ 2");
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertFalse(isValid);
  }

  @Test
  public void testInvalidFilterOperator() {
    List<String> filters = new LinkedList<String>();
    filters.add("country EQd india");
    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertFalse(isValid);
  }

  @Test
  public void testValidFilters() {
    List<String> filters = new LinkedList<String>();
    filters.add("country EQ india");
    // invalid filter - because of invalid dimension
    filters.add("revenue GT 200");

    Boolean isValid = filtersValidator.isValid(filters, context);

    Assert.assertFalse(isValid);
  }
}
