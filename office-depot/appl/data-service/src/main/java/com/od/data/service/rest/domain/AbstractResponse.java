package com.od.data.service.rest.domain;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonFormat.Shape;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.od.data.service.rest.domain.model.ErrorResponse;

/**
 * @author sandeep
 * 
 */
public abstract class AbstractResponse {
  @JsonInclude(Include.NON_NULL)
  private Query query;
  @JsonInclude(Include.NON_EMPTY)
  private List<ColumnHeader> columnHeaders;
  @JsonInclude(Include.NON_EMPTY)
  @JsonFormat(shape = Shape.OBJECT)
  private List<String[]> rows;
  @JsonInclude(Include.NON_NULL)
  private ErrorResponse errorResponse;

  public Query getQuery() {
    return query;
  }

  public void setQuery(Query query) {
    this.query = query;
  }

  public List<ColumnHeader> getColumnHeaders() {
    if (columnHeaders == null && rows != null && !rows.isEmpty()) {
      columnHeaders = Query.createColumnHeaders(this.query, true);
    }
    return columnHeaders;
  }

  public List<String[]> getRows() {
    return rows;
  }

  public void setRows(List<String[]> rows) {
    this.rows = rows;
  }

  public ErrorResponse getErrorResponse() {
    return errorResponse;
  }

  public void setErrorResponse(ErrorResponse errorResponse) {
    this.errorResponse = errorResponse;
  }
}
