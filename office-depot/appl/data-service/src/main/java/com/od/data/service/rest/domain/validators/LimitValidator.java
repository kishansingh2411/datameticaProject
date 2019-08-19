package com.od.data.service.rest.domain.validators;

import com.od.data.service.rest.domain.annotations.Limit;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * @author sandeep
 *
 */
public class LimitValidator implements ConstraintValidator<Limit, Integer> {

  @Override
  public void initialize(Limit constraintAnnotation) {
    
  }

  @Override
  public boolean isValid(Integer value, ConstraintValidatorContext context) {
    Boolean isValid = Boolean.FALSE;
    
    if (value == null || value.intValue() == -1 || value.intValue() > 0) {
      isValid = Boolean.TRUE;
    }
    return isValid;
  }

}
