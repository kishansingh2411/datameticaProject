package com.od.data.service.rest.domain.model;

import java.util.Collection;

/**
 * @author sandeep
 * 
 */
public class ErrorResponse {

  private Collection<Error> errors;

  public Collection<Error> getErrors() {
    return errors;
  }

  public void setErrors(Collection<Error> errors) {
    this.errors = errors;
  }

}
