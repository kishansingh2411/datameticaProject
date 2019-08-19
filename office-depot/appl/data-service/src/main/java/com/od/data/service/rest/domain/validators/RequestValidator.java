package com.od.data.service.rest.domain.validators;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.DataType;
import com.od.data.service.rest.domain.Dimension;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.annotations.RequestValidation;
import com.od.data.service.rest.domain.model.ErrorCode;
import com.od.data.service.util.DimensionUtil;
import com.od.data.service.util.QuarterDateFormat;

/**
 * @author sandeep
 * 
 */
public class RequestValidator implements ConstraintValidator<RequestValidation, Request> {

  @Override
  public void initialize(RequestValidation constraintAnnotation) {

  }

  @Override
  public boolean isValid(Request value, ConstraintValidatorContext context) {

    boolean isValidDate = isValidDateRangeAndDimension(value, context);

    return isValidDate;
  }

  private boolean isValidDateRangeAndDimension(Request value, ConstraintValidatorContext context) {
    boolean isValid = false;
    String sFromDate = value.getFromDate();
    String sToDate = value.getToDate();
    String appliedPattern = null;

    if (null == sFromDate || null == sToDate || sFromDate.length() != sToDate.length()) {
      context.disableDefaultConstraintViolation();
      context.buildConstraintViolationWithTemplate(ErrorCode.INVALID_DATE_FORMAT.name()).addConstraintViolation();
      isValid = false;
    } else {
      sFromDate = sFromDate.trim();
      sToDate = sToDate.trim();
      isValid = true;
    }

    if (isValid) {
      appliedPattern = getAppliedDatePattern(sFromDate, sToDate);
      if (null == appliedPattern) {
        context.disableDefaultConstraintViolation();
        context.buildConstraintViolationWithTemplate(ErrorCode.INVALID_DATE_FORMAT.name()).addConstraintViolation();
        isValid = false;
      } else {
        isValid = isValidDateDimension(value, context, appliedPattern);
      }
    }

    return isValid;

  }

  private String getAppliedDatePattern(String fromDate, String toDate) {
    AggregatorLevel[] aggregatorLevels = AggregatorLevel.values();
    String appliedPattern = null;
    for (AggregatorLevel level : aggregatorLevels) {
      for (String pattern : level.getValidRequestParams()) {
        try {
          DateFormat df;
          if (AggregatorLevel.QUARTER == level) {
            df = new QuarterDateFormat(pattern);
          } else {
            df = new SimpleDateFormat(pattern);
          }
          df.setLenient(Boolean.FALSE);
          df.parse(fromDate);
          df.parse(toDate);
          appliedPattern = pattern;
        } catch (Exception e) {
          // do nothing
        }
        if (null != appliedPattern) {
          break;
        }
      }
      if (null != appliedPattern) {
        break;
      }
    }

    return appliedPattern;

  }

  private boolean isValidDateDimension(Request value, ConstraintValidatorContext context, String appliedPattern) {
    boolean isValid = true;
    String strDimensions = value.getDimensions();
    if (null != strDimensions && !strDimensions.trim().isEmpty()) {

      Map<String, List<AggregatorLevel>> patternToAggregatorLevelsMap = AggregatorLevel.getPatternToAggregatorLevelsMap(false);
      List<AggregatorLevel> applicableAggregatorLevels = patternToAggregatorLevelsMap.get(appliedPattern);

      String[] sDimArray = strDimensions.trim().split(",");
      AggregatorLevel passedDim = null;
      for (String sDim : sDimArray) {
        Dimension dimension = DimensionUtil.getDimension(value.getModule(), sDim);
        if (dimension != null && DataType.DATE == dimension.getDatatype()) {
          passedDim = AggregatorLevel.getAggregateLevel(sDim);
          break;
        }
      }
      if (null != passedDim) {
        if (!applicableAggregatorLevels.contains(passedDim)) {
          isValid = false;
          context.disableDefaultConstraintViolation();
          context.buildConstraintViolationWithTemplate(ErrorCode.INVALID_DATE_FORMAT.name()).addConstraintViolation();
        }
      }
    }
    return isValid;
  }

}
