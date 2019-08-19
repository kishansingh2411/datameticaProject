package com.od.data.service.rest.domain;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;

/**
 * @author sandeep
 * 
 */
public class Response extends AbstractResponse {

  @JsonInclude(Include.NON_EMPTY)
  private String phoenixQuery;

  public String getPhoenixQuery() {
    return phoenixQuery;
  }

  public void setPhoenixQuery(String phoenixQuery) {
    this.phoenixQuery = phoenixQuery;
  }
}
