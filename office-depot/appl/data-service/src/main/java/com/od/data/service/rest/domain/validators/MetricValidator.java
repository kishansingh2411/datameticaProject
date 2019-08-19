package com.od.data.service.rest.domain.validators;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.annotations.Metric;
import com.od.data.service.util.MetricUtil;

/**
 * @author sandeep
 * 
 */
public class MetricValidator implements ConstraintValidator<Metric, String> {

  private Module module;

  public void initialize(Metric constraintAnnotation) {
    this.module = constraintAnnotation.module();
  }

  public boolean isValid(String value, ConstraintValidatorContext context) {
    Boolean isValid = Boolean.FALSE;
    if (value != null && !value.trim().isEmpty()) {
      String[] metricValues = value.trim().split(",");
      int validmetric = 0;
      for (String metric : metricValues) {
        if (!MetricUtil.isSupported(module, metric.trim())) {
          break;
        }
        validmetric++;
      }
      if (metricValues.length == validmetric) {
        isValid = Boolean.TRUE;
      }
    } else {
      isValid = Boolean.TRUE;
    }

    return isValid;
  }
}
