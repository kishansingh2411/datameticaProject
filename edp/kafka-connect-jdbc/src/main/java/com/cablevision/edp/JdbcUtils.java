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
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cablevision.edp.bo.DDPAuditResult;

/**
 * Utilties for interacting with a JDBC database.
 */
public class JdbcUtils {

  private static final Logger log = LoggerFactory.getLogger(JdbcUtils.class);

  /**
   * The default table types to include when listing tables if none are specified. Valid values
   * are those specified by the @{java.sql.DatabaseMetaData#getTables} method's TABLE_TYPE column.
   * The default only includes standard, user-defined tables.
   */
  public static final Set<String> DEFAULT_TABLE_TYPES = Collections.unmodifiableSet(
      new HashSet<String>(Arrays.asList("TABLE","VIEW"))
  );

  private static final int GET_TABLES_TYPE_COLUMN = 4;
  private static final int GET_TABLES_NAME_COLUMN = 3;
  private static final int GET_TABLES_SCHEMA_COLUMN = 2;
  
  private static final int GET_COLUMNS_COLUMN_NAME = 4;
  private static final int GET_COLUMNS_IS_NULLABLE = 18;
  private static final int GET_COLUMNS_IS_AUTOINCREMENT = 23;
  
  private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  private static ThreadLocal<SimpleDateFormat> DATE_FORMATTER = new ThreadLocal<SimpleDateFormat>() {
    @Override
    protected SimpleDateFormat initialValue() {
      SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
      sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
      return sdf;
    }
  };

  /**
   * Get a list of tables in the database. This uses the default filters, which only include
   * user-defined tables.
   * @param conn database connection
   * @return a list of tables
   * @throws SQLException
   */
  public static List<String> getTables(Connection conn) throws SQLException {
   List<String> tables = getTables(conn, DEFAULT_TABLE_TYPES);
//   log.info("tables returned "+tables);
   return tables;
  }

  /**
   * Get a list of table names in the database.
   * @param conn database connection
   * @param types a set of table types that should be included in the results
   * @throws SQLException
   */
  public static List<String> getTables(Connection conn, Set<String> types) throws SQLException {
    DatabaseMetaData metadata = conn.getMetaData();
    ResultSet rs = metadata.getTables(null, null, "%", null);
    log.info("resultset metadata column count "+rs.getMetaData().getColumnCount());
    log.info("resultset metadata table name "+rs.getMetaData().getTableName(1));
    List<String> tableNames = new ArrayList<String>();
    try {
		while (rs.next()) {
		  if (types.contains(rs.getString(GET_TABLES_TYPE_COLUMN))) {
		    String colName = rs.getString(GET_TABLES_NAME_COLUMN);
		    String tableSchemaName = rs.getString(GET_TABLES_SCHEMA_COLUMN);

		    // SQLite JDBC driver does not correctly mark these as system tables
		    if (metadata.getDatabaseProductName().equals("SQLite") && colName.startsWith("sqlite_")) {
		      continue;
		    }
		    
		    if (tableSchemaName != null && !tableSchemaName.equals("SYS") && !tableSchemaName.equals("SYSTEM") && !tableSchemaName.equals(metadata.getUserName()))
		    {
		    	tableNames.add(tableSchemaName+"."+colName);
		    }
		    else
		    {
		    	tableNames.add(colName);
		    }
		  }
		}
	} finally {
		if (rs != null)
			rs.close();
	}
    
    return tableNames;
  }
  

  /**
   * Look up the autoincrement column for the specified table.
   * @param conn database connection
   * @param table the table to
   * @return the name of the column that is an autoincrement column, or null if there is no
   *         autoincrement column or more than one exists
   * @throws SQLException
   */
  public static String getAutoincrementColumn(Connection conn, String table) throws SQLException {
    String result = null;
    int matches = 0;

    ResultSet rs = conn.getMetaData().getColumns(null, null, table, "%");
    // Some database drivers (SQLite) don't include all the columns
    try {
		if (rs.getMetaData().getColumnCount() >= GET_COLUMNS_IS_AUTOINCREMENT) {
		  while(rs.next()) {
		    if (rs.getString(GET_COLUMNS_IS_AUTOINCREMENT).equals("YES")) {
		      result = rs.getString(GET_COLUMNS_COLUMN_NAME);
		      matches++;
		    }
		  }
		  return (matches == 1 ? result : null);
		}
	} finally {
		if (rs != null)
			rs.close();
	}

    // Fallback approach is to query for a single row. This unfortunately does not work with any
    // empty table
    log.trace("Falling back to SELECT detection of auto-increment column for {}:{}", conn, table);
    Statement stmt = conn.createStatement();
    try {
      String quoteString = getIdentifierQuoteString(conn);
      rs = stmt.executeQuery("SELECT * FROM " + quoteString + table + quoteString + " LIMIT 1");
      ResultSetMetaData rsmd = rs.getMetaData();
      for(int i = 1; i < rsmd.getColumnCount(); i++) {
        if (rsmd.isAutoIncrement(i)) {
          result = rsmd.getColumnName(i);
          matches++;
        }
      }
    } finally {
      rs.close();
      stmt.close();
    }
    return (matches == 1 ? result : null);
  }

  public static boolean isColumnNullable(Connection conn, String table, String column)
      throws SQLException {
    ResultSet rs = conn.getMetaData().getColumns(null, null, table, column);
    try {
		if (rs.getMetaData().getColumnCount() > GET_COLUMNS_IS_NULLABLE) {
		  // Should only be one match
		  if (!rs.next()) {
		    return false;
		  }
		  String val = rs.getString(GET_COLUMNS_IS_NULLABLE);
		  return rs.getString(GET_COLUMNS_IS_NULLABLE).equals("YES");
		}
	} finally {
		if (rs != null)
			rs.close();
	}

    return false;
  }

  /**
   * Format the given Date assuming UTC timezone in a format supported by SQL.
   * @param date the date to convert to a String
   * @return the formatted string
   */
  public static String formatUTC(Date date) {
    return DATE_FORMATTER.get().format(date);
  }

  /**
   * Get the string used for quoting identifiers in this database's SQL dialect.
   * @param connection the database connection
   * @return the quote string
   * @throws SQLException
   */
  public static String getIdentifierQuoteString(Connection connection) throws SQLException {
    String quoteString = connection.getMetaData().getIdentifierQuoteString();
    quoteString = quoteString == null ? "" : quoteString;
    return quoteString;
  }

  /**
   * Quote the given string.
   * @param orig the string to quote
   * @param quote the quote character
   * @return the quoted string
   */
  public static String quoteString(String orig, String quote) {
    return quote + orig + quote;
  }
  
  public DDPAuditResult retriveAuditDetails(Connection conn,  String auditTableName, String tableName){
	  ResultSet rs = null;
	  PreparedStatement stmt = null;
	  DDPAuditResult result = null;
	  log.info("pulling data for tableName...."+tableName);
	  try {
		  String sql = "SELECT * FROM " +auditTableName+ " where table_name='" + tableName + "'";
		  stmt = conn.prepareStatement(sql);
		  rs = stmt.executeQuery();
		  while(rs.next()) {
			  result = new DDPAuditResult();
			  result.setTableName(rs.getString("table_name"));
			  result.setMaxSequence(Long.parseLong(rs.getString("max_sequence")));
			  if (rs.getObject("load_end_date") != null)
				  result.setLoadEndDate(formatter.parse(rs.getString("load_end_date")));
	      }
	    } catch (SQLException e) {
			log.error(e.getMessage(), e);
		} catch (ParseException e) {
			log.error(e.getMessage(), e);
		} finally {
	      try {
			rs.close();
			stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    }
	return result;
  }
  
  public boolean storeAuditDetails(Connection conn, String auditTableName, String tableName, long minSeqId, long maxSeqId, 
		  Date loadStartDate, Date loadEndDate){
	  PreparedStatement stmt = null;
	  String sql = null;
	  boolean updateFlag = false;
	  log.info("min seq id "+minSeqId+", max seq id "+maxSeqId+", load start date "+loadStartDate+", load end date "+loadEndDate);
	  log.info("storing data for tableName...."+tableName);
	  try {
		  
		  sql = "update " +auditTableName+ " set min_sequence= ?, max_sequence= ?, load_start_date= ? "+
			  		 ", load_end_date= ?, last_modified=now() where table_name='" + tableName + "'";
		  
		  stmt = conn.prepareStatement(sql);
			  
		  stmt.setLong(1, minSeqId);
		  stmt.setLong(2, maxSeqId);
		  if (loadStartDate == null)
		  {
			  stmt.setNull(3, java.sql.Types.TIMESTAMP);
		  }
		  else
		  {
			  stmt.setTimestamp(3, new java.sql.Timestamp(loadStartDate.getTime()));
		  }
		  
		  if (loadEndDate != null)
		  {
			  stmt.setTimestamp(4, new java.sql.Timestamp(loadEndDate.getTime()));
		  }
		  else
		  {
			  stmt.setNull(4, java.sql.Types.TIMESTAMP);
		  }
		  
		   int updateStatus = stmt.executeUpdate();
		   if(updateStatus > 0){
			   log.info("Table " + tableName + " updated succesfully.");
			   updateFlag = true;
		   }
	    } catch (SQLException e) {
			e.printStackTrace();
		} finally {
	      try {
			stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	    }
	  return updateFlag;
  }
  
  public static void main(String[] args) {
	Connection conn;
	try {
		conn = DriverManager.getConnection("jdbc:postgresql://cvldhdpan1.cscdev.com:5432/pgedhd?user=edhmgr&password=Edhmgr_dev1");
		JdbcUtils util = new JdbcUtils();
		//calling retriveAuditDetails method
		//log.info(util.retriveAuditDetails(conn, "ddp_audit","edp_code999").toString());
		//calling storeAuditDetails method
		boolean flag = util.storeAuditDetails(conn, "ddp_audit", "edp_test", 20, 139, new Date() , new Date());
		log.info("Update status is "+flag);
		
	} catch (SQLException e) {
		e.printStackTrace();
	}
}
}

