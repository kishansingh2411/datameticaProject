package com.od.data.service.rest.handler;

import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.validation.ObjectError;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.od.data.service.rest.domain.Query;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.rest.domain.model.Error;
import com.od.data.service.rest.domain.model.ErrorCode;
import com.od.data.service.rest.domain.model.ErrorResponse;

/**
 * @author sandeep
 * 
 */
@ControllerAdvice
public class ErrorHandler {

  @Autowired
  private ResourceBundleMessageSource messageSource;

  @ExceptionHandler(BindException.class)
  @ResponseStatus(value = HttpStatus.BAD_REQUEST)
  @ResponseBody
  public Response processErrors(BindException ex) {
    BindingResult result = ex.getBindingResult();
    List<ObjectError> objectErrors = result.getAllErrors();
    Response clickStreamResponse = new Response();
    Object request = ex.getTarget();
    Query query = null;
    if (request instanceof Request) {
      query = new Query((Request) request, Boolean.FALSE);
    }

    ErrorResponse errorResponse = processObjectErrors(objectErrors);
    clickStreamResponse.setQuery(query);
    clickStreamResponse.setErrorResponse(errorResponse);

    return clickStreamResponse;
  }

  private ErrorResponse processObjectErrors(List<ObjectError> objectErrors) {
    ErrorResponse errorResponse = new ErrorResponse();
    Set<Error> errors = new HashSet<Error>(objectErrors.size());
    for (ObjectError objectError : objectErrors) {
      Error error = new Error();
      ErrorCode code = ErrorCode.valueOf(objectError.getDefaultMessage());
      error.setErrorCode(code);
      error.setErrorDescription(resolveLocalizedMessage(code.description()));
      errors.add(error);
    }
    errorResponse.setErrors(errors);

    return errorResponse;
  }

  private String resolveLocalizedMessage(String messageCode) {
    Locale currentLocale = LocaleContextHolder.getLocale();
    Object[] arguments = null;
    String localizedMessage = messageSource.getMessage(messageCode, arguments, currentLocale);

    return localizedMessage;
  }

}
