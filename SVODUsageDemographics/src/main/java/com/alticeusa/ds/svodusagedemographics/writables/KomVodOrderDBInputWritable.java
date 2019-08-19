package com.alticeusa.ds.svodusagedemographics.writables;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.*;

/**
 * Created by ksingh on 5/18/16.
 */
public class KomVodOrderDBInputWritable implements Writable, DBWritable
{
  private Long customerAccountId;
  private Long vodOrderId;
  private Long corp;
  private String previewInd;
  private String trailerInd;
  private Timestamp dtmCreated;

  public void readFields(DataInput in) throws IOException {   }

  public void readFields(ResultSet rs) throws SQLException
  //Resultset object represents the data returned from a SQL statement
  {
    customerAccountId = rs.getLong( 1);
    vodOrderId = rs.getLong( 2);
    corp = rs.getLong( 3);
    previewInd = rs.getString( 4);
    trailerInd = rs.getString( 5);
    dtmCreated = rs.getTimestamp( 6);
  }

  public void write(DataOutput out) throws IOException {  }

  public void write(PreparedStatement ps) throws SQLException
  {
    ps.setLong( 1, customerAccountId);
    ps.setLong( 2, vodOrderId);
    ps.setLong( 3, corp);
    ps.setString( 4, previewInd);
    ps.setString( 5, trailerInd);
    ps.setTimestamp( 6, dtmCreated);
  }

  public Long getCustomerAccountId( )
  {
    return customerAccountId;
  }

  public Long getVodOrderId( )
  {
    return vodOrderId;
  }

  public Long getCorp( )
  {
    return corp;
  }

  public String getPreviewInd( )
  {
    return previewInd;
  }

  public String getTrailerInd( )
  {
    return trailerInd;
  }

  public Timestamp getdtmCreated( )
  {
    return dtmCreated;
  }

}
