package com.od.data.service.rest.domain.validators;

import java.util.List;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.FilterOperator;
import com.od.data.service.rest.domain.Metric;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.annotations.Filters;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.MetricUtil;

/**
 * @author sandeep
 * 
 */
public class FiltersValidator implements ConstraintValidator<Filters, List<String>> {

  private Module module;

  public void initialize(Filters constraintAnnotation) {
    this.module = constraintAnnotation.module();
  }

  public boolean isValid(List<String> values, ConstraintValidatorContext context) {
    Boolean isValid = Boolean.FALSE;

    if (values != null && !values.isEmpty()) {
      int validValueCount = 0;
      for (String value : values) {
        String[] orFilters = value.split(",");
        int i = 0;
        for (; i < orFilters.length; i++) {
          if (!isValidFilter(orFilters[i])) {
            break;
          }
        }
        if (i == orFilters.length) {
          validValueCount++;
        }
      }

      if (validValueCount == values.size()) {
        isValid = Boolean.TRUE;
      }
    } else {
      isValid = Boolean.TRUE;
    }

    return isValid;
  }

  private boolean isValidFilter(String filter) {
    boolean isValid = Boolean.FALSE;
    String[] valueArray = filter.trim().split(" ");
    if (valueArray.length == 3) {
      String attribute = valueArray[0].trim();
      String operationString = valueArray[1].trim();
      String filterString = valueArray[2].trim();
      FilterOperator filterOperator = FilterOperator.getFilterOperator(operationString);
      Dimension dimension = DimensionUtil.getDimension(module, attribute);
      Metric metric = MetricUtil.getMetric(module, attribute);

      if (filterOperator != null && filterString.length() > 0) {
        if (dimension != null) {
          return Boolean.TRUE;
        } else if (metric != null) {
          return Boolean.TRUE;
        }
      }
    }
    return isValid;
  }
}
