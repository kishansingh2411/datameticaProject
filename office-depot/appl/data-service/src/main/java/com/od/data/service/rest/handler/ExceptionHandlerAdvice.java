package com.od.data.service.rest.handler;

import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.i18n.LocaleContextHolder;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

import com.od.data.service.rest.domain.Response;
import com.od.data.service.rest.domain.model.Error;
import com.od.data.service.rest.domain.model.ErrorCode;
import com.od.data.service.rest.domain.model.ErrorResponse;

/**
 * @author sandeep
 * 
 */
@ControllerAdvice
public class ExceptionHandlerAdvice {

  private static final Logger LOGGER = Logger.getLogger(ExceptionHandlerAdvice.class);
  @Autowired
  private ResourceBundleMessageSource messageSource;

  @ExceptionHandler(RuntimeException.class)
  @ResponseStatus(value = HttpStatus.INTERNAL_SERVER_ERROR)
  @ResponseBody
  public Response processErrors(RuntimeException ex) {
    Response response = new Response();
    ErrorResponse errorResponse = new ErrorResponse();
    response.setErrorResponse(errorResponse);
    Set<Error> errors = new HashSet<Error>(1);
    Error error = new Error();
    ErrorCode code = ErrorCode.INTERNAL_SERVER_ERROR;
    error.setErrorCode(code);
    error.setErrorDescription(resolveLocalizedMessage(code.description()));
    errors.add(error);
    errorResponse.setErrors(errors);
    LOGGER.error(error.getErrorDescription(), ex);
    return response;
  }

  private String resolveLocalizedMessage(String messageCode) {
    Locale currentLocale = LocaleContextHolder.getLocale();
    Object[] arguments = null;
    String localizedMessage = messageSource.getMessage(messageCode, arguments, currentLocale);

    return localizedMessage;
  }

}
