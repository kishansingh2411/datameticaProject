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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cablevision.edp.bo.DDPAuditResult;
import com.cablevision.edp.util.Version;

import io.confluent.common.config.ConfigException;
import io.confluent.common.utils.SystemTime;
import io.confluent.common.utils.Time;

/**
 * JdbcSourceTask is a Kafka Connect SourceTask implementation that reads from JDBC databases and
 * generates Kafka Connect records.
 */
public class CustomJdbcSourceTask extends SourceTask {

  private static final Logger log = LoggerFactory.getLogger(CustomJdbcSourceTask.class);

  static final String INCREMENTING_FIELD = "incrementing";
  static final String TIMESTAMP_FIELD = "timestamp";
  
  private Time time;
  private JdbcSourceTaskConfig config;
  private Connection db;
  private Connection offsetDb;
  private PriorityQueue<CustomTableQuerier> tableQueue = new PriorityQueue<CustomTableQuerier>();
  private AtomicBoolean stop;
  private HashMap<Map, Map> offsetMap = new HashMap<Map, Map>();
  JdbcUtils jdbcUtils = new JdbcUtils();
  
  public CustomJdbcSourceTask() {
    this.time = new SystemTime();
  }

  public CustomJdbcSourceTask(Time time) {
    this.time = time;
  }

  @Override
  public String version() {
    return Version.getVersion();
  }

  @Override
  public void start(Map<String, String> properties) {
	
    try {
      config = new JdbcSourceTaskConfig(properties);
    } catch (ConfigException e) {
      throw new ConnectException("Couldn't start JdbcSourceTask due to configuration error", e);
    }
    
    List<String> whiteListTables = config.getList(JdbcSourceTaskConfig.TABLE_WHITELIST_CONFIG);
    List<String> tables = config.getList(JdbcSourceTaskConfig.TABLES_CONFIG);
    String query = config.getString(JdbcSourceTaskConfig.QUERY_CONFIG);
    if ((tables.isEmpty() && query.isEmpty()) || (!tables.isEmpty() && !query.isEmpty())) {
      throw new ConnectException("Invalid configuration: each JdbcSourceTask must have at "
                                        + "least one table assigned to it or one query specified");
    }
    
    CustomTableQuerier.QueryMode queryMode = !query.isEmpty() ? CustomTableQuerier.QueryMode.QUERY :
                                       CustomTableQuerier.QueryMode.TABLE;
    List<String> tablesOrQuery = queryMode == CustomTableQuerier.QueryMode.QUERY ?
                                 Collections.singletonList(query) : tables;

    String mode = config.getString(JdbcSourceTaskConfig.MODE_CONFIG);
 
    List<String> topicList = config.getList(JdbcSourceTaskConfig.TOPICS_CONFIG);
    List<String> incrementingColumnList = config.getList(JdbcSourceTaskConfig.INCREMENTING_COLUMN_NAME_CONFIG);
    List<String> timestampColumnList = config.getList(JdbcSourceTaskConfig.TIMESTAMP_COLUMN_NAME_CONFIG);
    
    if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP) ||
        mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
    	
      List<Map<String, String>> partitions = new ArrayList<>(tables.size());
      
      switch (queryMode) {
        case TABLE:
          for (String table : tables) {
            Map<String, String> partition =
                Collections.singletonMap(JdbcSourceConnectorConstants.TABLE_NAME_KEY, table);
            partitions.add(partition);
          }
          break;
        case QUERY:
          partitions.add(Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                                  JdbcSourceConnectorConstants.QUERY_NAME_VALUE));
          break;
      }
      
      log.info("Kafka maintained offsets "+context.offsetStorageReader().offsets(partitions));
      
      
    }

    // Must setup the connection now to validate NOT NULL columns. At this point we've already
    // caught any easy-to-find errors so deferring the connection creation won't save any effort
    String dbUrl = config.getString(JdbcSourceTaskConfig.CONNECTION_URL_CONFIG);
    log.info("Trying to connect to {}", dbUrl);
    try {
    	
      db = DriverManager.getConnection(dbUrl);
      
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", dbUrl, e);
      throw new ConnectException(e);
    }

    String offsetDbUrl = config.getString(JdbcSourceConnectorConfig.OFFSET_DB_CONNECTION_URL_CONFIG);
    
    log.debug("Trying to connect to {}", offsetDbUrl);
    try {
    	
    	offsetDb = DriverManager.getConnection(offsetDbUrl);
      
    } catch (SQLException e) {
      log.error("Couldn't open connection to {}: {}", offsetDbUrl, e);
      throw new ConnectException(e);
    }
    
    
    for (String table : tables) {
    	
    	DDPAuditResult auditResult = jdbcUtils.retriveAuditDetails(offsetDb, "ddp_audit", table.toLowerCase());
    	if (auditResult == null)
    	{
    		log.error("Could not retrieve last offset value for table "+table+" from the audit table");
    		throw new ConnectException("Could not retrieve last offset value for table "+table+" from the audit table");
    	}
    	
    	Date endLoadDate = auditResult.getLoadEndDate();
    	long maxSeq = auditResult.getMaxSequence();
    	Map srcPartition = new HashMap();
    	srcPartition.put(JdbcSourceConnectorConstants.TABLE_NAME_KEY, table);
    	
    	Map srcOffset = new HashMap();
    	if (endLoadDate != null)
    		srcOffset.put("END_LOAD_DATE", endLoadDate.getTime());
    	
		srcOffset.put(INCREMENTING_FIELD, maxSeq);
		offsetMap.put(srcPartition, srcOffset);
    }
    
    String queryHint = config.getString(JdbcSourceConnectorConfig.QUERY_HINT);
    String additionalQueryCondition = config.getString(JdbcSourceConnectorConfig.QUERY_ADDITIONAL_CONDITION);

   
    String topicPrefix = config.getString(JdbcSourceTaskConfig.TOPIC_PREFIX_CONFIG);
    
    
    for (String tableOrQuery : tablesOrQuery) {
      final Map<String, String> partition;
      switch (queryMode) {
        case TABLE:
//          validateNonNullable(mode, tableOrQuery, incrementingColumnList, timestampColumnList, whiteListTables);
          partition = Collections.singletonMap(
              JdbcSourceConnectorConstants.TABLE_NAME_KEY, tableOrQuery);
          break;
        case QUERY:
          partition = Collections.singletonMap(JdbcSourceConnectorConstants.QUERY_NAME_KEY,
                                               JdbcSourceConnectorConstants.QUERY_NAME_VALUE);
          break;
        default:
          throw new ConnectException("Unexpected query mode: " + queryMode);
      }
      
      Map<String, Object> offset = offsetMap == null ? null : offsetMap.get(partition);
      log.info("offset fetched "+offset);
      Long incrementingOffset = offset == null ? null :
                              (Long)offset.get(INCREMENTING_FIELD);
      Long timestampOffset = offset == null ? null :
                             (Long)offset.get(TIMESTAMP_FIELD);
      
      Date endLoadDate = null;
      
      if (offset.get("END_LOAD_DATE") != null)
    	  endLoadDate = new java.util.Date((Long) offset.get("END_LOAD_DATE"));


      	if (mode.equals(JdbcSourceTaskConfig.MODE_INCREMENTING)) {
    	  
    	  String incrementingColumn = "";
    	  
    	  log.info("table "+tableOrQuery);
    	  log.info("white list tables "+whiteListTables);
    	  if (whiteListTables != null && !whiteListTables.isEmpty())
    	  {
    		  incrementingColumn = incrementingColumnList.get(whiteListTables.indexOf(tableOrQuery));
    	  }
    	  else
    	  {
    		  incrementingColumn = incrementingColumnList.get(0);
    	  }

    	  tableQueue.add(new GenericTimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, null, null, incrementingColumn, incrementingOffset, additionalQueryCondition, queryHint, 
            							endLoadDate, topicList.get(whiteListTables.indexOf(tableOrQuery))));
        
      } else if (mode.equals(JdbcSourceTaskConfig.MODE_TIMESTAMP)) {
    	  
    	  String timestampColumn = "";
    	  
    	  if (whiteListTables != null && !whiteListTables.isEmpty())
    	  {
    		  timestampColumn = timestampColumnList.get(whiteListTables.indexOf(tableOrQuery));
    	  }
    	  else
    	  {
    		  timestampColumn = timestampColumnList.get(0);
    	  }

    	  tableQueue.add(new GenericTimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, timestampColumn, timestampOffset, 
            	null, null, additionalQueryCondition, queryHint, endLoadDate, topicList.get(whiteListTables.indexOf(tableOrQuery))));
        
      } else if (mode.endsWith(JdbcSourceTaskConfig.MODE_TIMESTAMP_INCREMENTING)) {
    	  
    	  String incrementingColumn = "";
    	  
    	  if (whiteListTables != null && !whiteListTables.isEmpty())
    	  {
    		  incrementingColumn = incrementingColumnList.get(whiteListTables.indexOf(tableOrQuery));
    	  }
    	  else
    	  {
    		  incrementingColumn = incrementingColumnList.get(0);
    	  }
    	  
    	  String timestampColumn = "";
    	  
    	  if (whiteListTables != null && !whiteListTables.isEmpty())
    	  {
    		  timestampColumn = timestampColumnList.get(whiteListTables.indexOf(tableOrQuery));
    	  }
    	  else
    	  {
    		  timestampColumn = timestampColumnList.get(0);
    	  }
    	  
    	  tableQueue.add(new GenericTimestampIncrementingTableQuerier(
            queryMode, tableOrQuery, topicPrefix, timestampColumn, timestampOffset,
            incrementingColumn, incrementingOffset, additionalQueryCondition, queryHint, endLoadDate, topicList.get(whiteListTables.indexOf(tableOrQuery))));
      }
    }

    stop = new AtomicBoolean(false);
  }

  @Override
  public void commit() throws InterruptedException
  {
	  log.info("Committing offsets "+offsetMap);
	  
	  try {
		if (offsetDb == null || offsetDb.isClosed() || !offsetDb.isValid(0))
		  {
		  	 log.debug("Trying to connect to {}", config.getString(JdbcSourceTaskConfig.OFFSET_DB_CONNECTION_URL_CONFIG));
		  	    try {
		  	    	
		  	    	offsetDb = DriverManager.getConnection(config.getString(JdbcSourceTaskConfig.OFFSET_DB_CONNECTION_URL_CONFIG));
		  	      
		  	    } catch (SQLException e) {
		  	      log.error("Couldn't open connection to {}: {}", config.getString(JdbcSourceTaskConfig.OFFSET_DB_CONNECTION_URL_CONFIG), e);
		  	      throw new ConnectException(e);
		  	    }
		  }
	} catch (SQLException e1) {
		log.error("Error encountered while trying to open connection to Audit database.");
		log.error(e1.getMessage(), e1);
	}
	  
	  try {
		for (Map.Entry<Map, Map>entry: offsetMap.entrySet())
		  {
			  String tableName = (String) entry.getKey().get(JdbcSourceConnectorConstants.TABLE_NAME_KEY);
			  long maxSeqId = (long) entry.getValue().get(INCREMENTING_FIELD);
			 
			  long minSeqId = 0;
			  if (entry.getValue().get("MIN_SEQ_ID") != null)
				  minSeqId = (long) entry.getValue().get("MIN_SEQ_ID");
			  
			  Date load_end_date = null;
			  if (entry.getValue().get("END_LOAD_DATE") != null)
			  	load_end_date = new java.util.Date((long) entry.getValue().get("END_LOAD_DATE"));
			  
			  Date start_end_date = null;
			  if (entry.getValue().get("START_LOAD_DATE") != null)
			  	start_end_date = new java.util.Date((long) entry.getValue().get("START_LOAD_DATE"));
			  
			  
			  jdbcUtils.storeAuditDetails(offsetDb, "ddp_audit", tableName.toLowerCase(), minSeqId ,maxSeqId,  start_end_date, load_end_date);
		  }
	} catch (Exception e) {
		log.error("Following error occured while trying to store the offset information "+offsetMap+" into audit table");
		log.error(e.getMessage(), e);
		
	}
	  
  }
  
  @Override
  public void commitRecord(SourceRecord record) throws InterruptedException
  {
	  log.info("Committing record offset for source partition "+record.sourcePartition() +", "+record.sourceOffset());
	  offsetMap.put(record.sourcePartition(), record.sourceOffset());
	  log.info("offset map "+offsetMap);
  }
  
  @Override
  public void stop() throws ConnectException {
    if (stop != null) {
      stop.set(true);
    }
    if (db != null) {
      log.debug("Trying to close database connection");
      try {
        db.close();
      } catch (SQLException e) {
        log.error("Failed to close database connection: ", e);
      }
    }
    
    if (offsetDb != null) {
        log.debug("Trying to close audit database connection");
        try {
        	offsetDb.close();
        } catch (SQLException e) {
          log.error("Failed to close audit database connection: ", e);
        }
      }
  }

  @Override
  public List<SourceRecord> poll() throws InterruptedException {
    long now = time.milliseconds();
    log.info("{} Polling for new data");
    while (!stop.get()) {
      // If not in the middle of an update, wait for next update time
      CustomTableQuerier querier = tableQueue.peek();
      if (!querier.querying()) {
        long nextUpdate = querier.getLastUpdate() +
                          config.getInt(JdbcSourceTaskConfig.POLL_INTERVAL_MS_CONFIG);
        long untilNext = nextUpdate - now;
        log.trace("Waiting {} ms to poll {} next", untilNext, querier.toString());
        if (untilNext > 0) {
          time.sleep(untilNext);
          now = time.milliseconds();
          // Handle spurious wakeups
          continue;
        }
      }

      List<SourceRecord> results = new ArrayList<>();
      try {
        log.trace("Checking for next block of results from {}", querier.toString());
        if (db == null || db.isClosed() || !db.isValid(0))
        {
        	 log.debug("Trying to connect to {}", config.getString(JdbcSourceTaskConfig.CONNECTION_URL_CONFIG));
        	    try {
        	    	
        	      db = DriverManager.getConnection(config.getString(JdbcSourceTaskConfig.CONNECTION_URL_CONFIG));
        	      
        	    } catch (SQLException e) {
        	      log.error("Couldn't open connection to {}: {}", config.getString(JdbcSourceTaskConfig.CONNECTION_URL_CONFIG), e);
        	      throw new ConnectException(e);
        	    }
        }
        
        querier.maybeStartQuery(db);

        int batchMaxRows = config.getInt(JdbcSourceTaskConfig.BATCH_MAX_ROWS_CONFIG);
        boolean hadNext = true;
        while (results.size() < batchMaxRows && (hadNext = querier.next())) {
          results.add(querier.extractRecord());
        }

        // If we finished processing the results from this query, we can clear it out
        if (!hadNext) {
          log.trace("Closing this query for {}", querier.toString());
          CustomTableQuerier removedQuerier = tableQueue.poll();
          assert removedQuerier == querier;
          now = time.milliseconds();
          querier.close(now);
          tableQueue.add(querier);
        }

        if (results.isEmpty()) {
          log.trace("No updates for {}", querier.toString());
          continue;
        }

        log.info("Returning {} records for {}", results.size(), querier.toString());

        return results;
      } catch (SQLException e) {
        log.error("Failed to run query for table {}: {}", querier.toString(), e);
        // clear out the query if we had errors, this also handles backoff in case of errors
        if (querier != null) {
          now = time.milliseconds();
          try {
            querier.close(now);
          } catch (SQLException e1) {
            log.error("Failed to close result set for failed query ", e);
          }
        }
        return null;
      }
    }

    // Only in case of shutdown
    return null;
  }

  private void validateNonNullable(String incrementalMode, String table, List<String> incrementingColumnList,
                                   List<String> timestampColumnList, List<String> whiteListTables) {
    try {
      // Validate that requested columns for offsets are NOT NULL. Currently this is only performed
      // for table-based copying because custom query mode doesn't allow this to be looked up
      // without a query or parsing the query since we don't have a table name.
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_INCREMENTING) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(db, table, incrementingColumnList.get(whiteListTables.indexOf(table)))) {
        throw new ConnectException("Cannot make incremental queries using incrementing column " +
        		incrementingColumnList.get(whiteListTables.indexOf(table)) + " on " + table + " because this column is "
                                   + "nullable.");
      }
      if ((incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP) ||
           incrementalMode.equals(JdbcSourceConnectorConfig.MODE_TIMESTAMP_INCREMENTING)) &&
          JdbcUtils.isColumnNullable(db, table, timestampColumnList.get(whiteListTables.indexOf(table)))) {
        throw new ConnectException("Cannot make incremental queries using timestamp column " +
        		timestampColumnList.get(whiteListTables.indexOf(table)) + " on " + table + " because this column is "
                                   + "nullable.");
      }
    } catch (SQLException e) {
      throw new ConnectException("Failed trying to validate that columns used for offsets are NOT"
                                 + " NULL", e);
    }
  }
}
