package com.od.data.service.rest.domain.validators;


import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.annotations.Dimension;
import com.od.data.service.util.DimensionUtil;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

/**
 * @author sandeep
 * 
 */
public class DimensionValidator implements ConstraintValidator<Dimension, String> {

  private Module module;

  public void initialize(Dimension constraintAnnotation) {
    this.module = constraintAnnotation.module();
  }

  public boolean isValid(String value, ConstraintValidatorContext context) {
    Boolean isValid = Boolean.FALSE;
    if (value != null && value.trim().length() > 0) {
      String[] dimensionValues = value.trim().split(",");
      int validDimensions = 0;
      for (String dimension : dimensionValues) {
        if (!DimensionUtil.isSupported(module, dimension.trim())) {
          break;
        }
        validDimensions++;
      }
      if (dimensionValues.length == validDimensions) {
        isValid = Boolean.TRUE;
      }
    } else {
      isValid = Boolean.TRUE;
    }

    return isValid;
  }
}
