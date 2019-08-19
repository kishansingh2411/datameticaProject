package com.alticeusa.ds.svodusagedemographics.writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;

public class KomCustomerAccountDBInputWritable implements Writable, DBWritable
{
  private Long customerAccountId;
  private Long corp;
  private String dwellingNbr;
  private String cust;
  private Timestamp dtmLastUpdated;
  private Timestamp dtmCreated;

  public void readFields(DataInput in) throws IOException {   }

  public void readFields(ResultSet rs) throws SQLException
  //Resultset object represents the data returned from a SQL statement
  {
    customerAccountId = rs.getLong( 1);
    corp = rs.getLong( 2);
    dwellingNbr = rs.getString( 3);
    cust = rs.getString( 4);
    dtmLastUpdated = rs.getTimestamp( 5);
    dtmCreated = rs.getTimestamp( 6);
  }

  public void write(DataOutput out) throws IOException {  }

  public void write(PreparedStatement ps) throws SQLException
  {
    ps.setLong( 1, customerAccountId);
    ps.setLong( 2, corp);
    ps.setString( 3, dwellingNbr);
    ps.setString( 4, cust);
    ps.setTimestamp( 5, dtmLastUpdated);
    ps.setTimestamp( 6, dtmCreated);
  }

  public Long getCustomerAccountId( )
  {
    return customerAccountId;
  }

  public Long getCorp( )
  {
    return corp;
  }

  public String getDwellingNbr( )
  {
    return dwellingNbr;
  }

  public String getCust( )
  {
    return cust;
  }

  public Timestamp getDtmLastUpdated( )
  {
    return dtmLastUpdated;
  }

  public Timestamp getDtmCreated( )
  {
    return dtmCreated;
  }
}