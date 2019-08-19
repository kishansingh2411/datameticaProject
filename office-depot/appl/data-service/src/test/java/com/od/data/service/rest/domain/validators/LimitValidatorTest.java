package com.od.data.service.rest.domain.validators;

import javax.validation.ConstraintValidatorContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.od.data.service.rest.domain.validators.LimitValidator;

/**
 * @author sandeep
 * 
 */
public class LimitValidatorTest {

  private LimitValidator limitValidator;
  private ConstraintValidatorContext context;

  @Before
  public void setup() {
    limitValidator = new LimitValidator();
    context = Mockito.mock(ConstraintValidatorContext.class);
  }

  @Test
  public void testIsValidForNull() {
    Integer limit = null;
    Boolean isValid = limitValidator.isValid(limit, context);
    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForPositiveLimit() {
    Integer limit = 1;
    Boolean isValid = limitValidator.isValid(limit, context);
    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsValidForAllRecords() {
    Integer limit = -1;
    Boolean isValid = limitValidator.isValid(limit, context);
    Assert.assertTrue(isValid);
  }

  @Test
  public void testIsInvalidForZeroLimit() {
    Integer limit = 0;
    Boolean isValid = limitValidator.isValid(limit, context);
    Assert.assertFalse(isValid);
  }

  @Test
  public void testIsInvalidForNegativeOtherThanAllRecords() {
    Integer limit = -2;
    Boolean isValid = limitValidator.isValid(limit, context);
    Assert.assertFalse(isValid);
  }
}
