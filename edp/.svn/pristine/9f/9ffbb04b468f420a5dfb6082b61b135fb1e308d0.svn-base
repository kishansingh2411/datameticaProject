/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package com.cablevision.edp;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cablevision.edp.DataConverter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract public class CustomTableQuerier implements Comparable<CustomTableQuerier> {
	private static final Logger log = LoggerFactory.getLogger(CustomTableQuerier.class);

  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final QueryMode mode;
  protected final String name;
  protected final String query;
  protected final String topicPrefix;
  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected Schema schema;

  public CustomTableQuerier(QueryMode mode, String nameOrQuery, String topicPrefix) {
    this.mode = mode;
    this.name = mode.equals(QueryMode.TABLE) ? nameOrQuery : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    if (stmt != null) {
      return stmt;
    }
    
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      log.debug("In method maybeStartQuery.....");	
      log.debug("preparing query statement");
      stmt = getOrCreatePreparedStatement(db);
      log.info("done preparing query statement");
      resultSet = executeQuery();
      log.debug("In method maybeStartQuery.....got name [ "+name);
      log.debug("In method maybeStartQuery.....got resultSet.getMetaData() [ "+resultSet.getMetaData());
      schema = DataConverter.convertSchema(name, resultSet.getMetaData());
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
	  log.debug("fetching next record for table "+name+" "+resultSet);
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  public void close(long now) throws SQLException {
    if (resultSet != null)
      resultSet.close();
    resultSet = null;
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
//    schema = null;

    lastUpdate = now;
  }

  @Override
  public int compareTo(CustomTableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.name.compareTo(other.name);
    }
  }
}
