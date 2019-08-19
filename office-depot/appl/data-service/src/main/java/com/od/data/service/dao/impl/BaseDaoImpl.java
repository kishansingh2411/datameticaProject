package com.od.data.service.dao.impl;

import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.AggregatorLevel;
import com.od.data.service.rest.domain.ColumnHeader;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import javax.sql.DataSource;

import java.util.List;

/**
 * @author sandeep
 * 
 */
@Repository
public class BaseDaoImpl implements BaseDao {

  private static final Logger LOGGER = Logger.getLogger(BaseDaoImpl.class);
  private static final long TEN_SECONDS_IN_MILLIS = 10000;
  private JdbcTemplate jdbcTemplate;

  @Autowired
  public void setDataSource(DataSource dataSource) {
    this.jdbcTemplate = new JdbcTemplate(dataSource);
  }

  @Override
  public List<String[]> getRows(String query, List<ColumnHeader> columnHeaders,
      AggregatorLevel aggregatorLevel) {
    long timeBeforeQuery = System.currentTimeMillis();
    List<String[]> rows =
        jdbcTemplate.query(query, new ResponseRowMapper(columnHeaders, aggregatorLevel));
    long timeTaken = System.currentTimeMillis() - timeBeforeQuery;
    if (timeTaken > TEN_SECONDS_IN_MILLIS) {
      LOGGER.info("getRows(): Executed query=<" + query + ">, time taken(mS)=" + timeTaken);
    }
    return rows;
  }
}
