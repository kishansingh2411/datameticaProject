package com.od.data.service.service;

import com.od.data.service.rest.domain.Query;

/**
 * @author sandeep
 * 
 */
public interface QueryBuilder {

  /**
   * 
   * @param query
   * @return
   */
  String buildQuery(Query query);

}
