package com.od.data.service.dao;

import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ColumnHeader;

import java.util.List;

/**
 * @author sandeep
 *
 */
public interface BaseDao {

  /**
   * 
   * @param query
   * @param columnHeaders
   * @param aggregatorLevel
   * @return
   */
  List<String[]> getRows(String query, List<ColumnHeader> columnHeaders,
                         AggregatorLevel aggregatorLevel);
}
