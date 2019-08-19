package com.od.data.service.service.impl;

import java.util.List;

import org.apache.log4j.Logger;

import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.Query;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.QueryBuilder;

/**
 * @author sandeep
 * 
 */
public class AbstractServiceImpl {

  private static final Logger LOGGER = Logger.getLogger(AbstractServiceImpl.class);

  protected QueryBuilder queryBuilder;
  protected BaseDao dao;

  public Response getRecords(Request request) {
    LOGGER.info("getRecords(): called with request for (Module)=(" + request.getModule() + ")");
    Query query = getQueryObject(request);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Formed query=" + query);
    }
    Response response = new Response();
    String jdbcQuery = queryBuilder.buildQuery(query);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Built jdbcQuery=" + jdbcQuery);
    }

    List<String[]> rows = dao.getRows(jdbcQuery, Query.createColumnHeaders(query), query.getAggregatorLevel());
    response.setQuery(query);
    response.setRows(rows);
    LOGGER.info("Returning " + (null == rows ? 0 : rows.size()) + " records for (Module)=(" + request.getModule() + ")");
    return response;
  }

  protected Query getQueryObject(Request request) {
    Query query = new Query(request, Boolean.TRUE);
    return query;
  }

  public Response getJDBCQuery(Request request) {
    Query query = getQueryObject(request);
    Response response = new Response();
    String jdbcQuery = queryBuilder.buildQuery(query);
    response.setQuery(query);
    response.setPhoenixQuery(jdbcQuery);

    return response;
  }
}
