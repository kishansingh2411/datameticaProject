package com.od.data.service.rest.domain.model;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * @author sandeep
 * 
 */
public class Error {

  @JsonProperty("code")
  private ErrorCode errorCode;
  @JsonProperty("description")
  private String errorDescription;

  public ErrorCode getErrorCode() {
    return errorCode;
  }

  public void setErrorCode(ErrorCode errorCode) {
    this.errorCode = errorCode;
  }

  public String getErrorDescription() {
    return errorDescription;
  }

  public void setErrorDescription(String errorDescription) {
    this.errorDescription = errorDescription;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((errorCode == null) ? 0 : errorCode.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Error other = (Error) obj;
    if (errorCode != other.errorCode)
      return false;
    return true;
  }
}
