package com.alticeusa.ds.svodusagedemographics.encryptor;

import com.alticeusa.ds.svodusagedemographics.avro.KomCustomerAccount;
import com.alticeusa.ds.svodusagedemographics.writables.KomCustomerAccountDBInputWritable;
import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;
import com.alticeusa.ds.svodusagedemographics.utils.Util;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

/**
 * Map Reduce job creates a connection with cvcdrp.cdrusr.kom_customer_account table
 * and encrypts the customer_account_id and the chc_id.
 *
 * CHC_ID is created by concatenating corp, dwelling_nbr, cust columns.
 * The original dwelling_nbr and cust columns have some extra spaces so those are trimmed
 * and some '0's are padded so they match when joined with chc_id in the d_ecohort_chc
 * and d_ethnic_chc tables.
 *
 * Encryption is done using the CVApplicationSecurity encryption framework developed by
 * Subbu Karuppiah.
 *
 * These columns are highly sensitive so must always be encrypted before landing into
 * HDFS or Hive.
 *
 * NOTE: Set number of mappers = equal to the number of connections to be made to the
 * oracle CVCDRP database. LIMIT to <= 5
 *
 * All the database configurations will be stored in a properties file.
 *
 * This Job is only run one time to bulk import the data from oracle - close to 20M
 *
 * Created by Kriti Singh on 5/25/16.
 */
public class KomCustomerAccountBulkEncryptor extends Configured implements Tool
{
	private static final Logger logger = LoggerFactory.getLogger(KomCustomerAccountBulkEncryptor.class);	
  private static final SimpleDateFormat dateFormat
      = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

  private static final String USER = "etlmgr";
  private static final String DEVICE = "VOD001";
  private static final String OUTPUT_SCHEMA = "output_schema";

  private enum JOB_COUNTERS
  {
    KOM_CUSTOMER_ACCOUNT_RECORDS,
  }

  public static class MyMapper extends
      Mapper<LongWritable,KomCustomerAccountDBInputWritable,Text,NullWritable>
  {
    @Override
    public void map(LongWritable key, KomCustomerAccountDBInputWritable value, Context context)
        throws IOException, InterruptedException
    {
      Long customerAccountId = value.getCustomerAccountId( );
      Long corp = value.getCorp( );
      String dwellingNbr = value.getDwellingNbr( );
      String cust = value.getCust( );
      Timestamp dtmLastUpdated = value.getDtmLastUpdated( );
      Timestamp dtmCreated = value.getDtmCreated( );
      String chcId = corp + dwellingNbr + cust;

      context.getCounter( KomCustomerAccountBulkEncryptor.JOB_COUNTERS.KOM_CUSTOMER_ACCOUNT_RECORDS).increment( 1);
      context.write( new Text( customerAccountId + "\t" + chcId + "\t" + dtmLastUpdated + "\t" + dtmCreated), NullWritable.get( ));
    }
  }

  public static class MyReducer extends
      Reducer<Text,NullWritable,AvroKey<GenericRecord>,NullWritable>
  {
    private CVDataController cvc;
    private Schema outputSchema;

    @Override
    protected void setup( Context context)
        throws IOException, InterruptedException
    {
      Configuration conf = context.getConfiguration();
      outputSchema = new Schema.Parser( ).parse( conf.get(OUTPUT_SCHEMA));

      try {
        cvc = new CVDataController(DEVICE, USER);
      } catch (CVApplicationException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void reduce( Text key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException
    {
      String[] fields = key.toString( ).split( "\\t");
      String customerAccountId =  fields[0];
      String chcId = fields[1];
      Date dtmLastUpdated = null;
      Date dtmCreated = null;

      try
      {
        dtmLastUpdated = (fields[2] != null) ? dateFormat.parse(fields[2]) : null;
        dtmCreated = (fields[3] != null) ? dateFormat.parse(fields[3]) : null;
      }
      catch( Exception e)
      {
        System.out.println( "Date Parse Exception = " + fields[2]);
      }

      String encryptedCustomerAccountId = "";
      String encryptedChcId = "";

      try
      {
        encryptedCustomerAccountId = cvc.encryptField("device", customerAccountId);
        encryptedChcId = cvc.encryptField("device", chcId);
      }
      catch( CVApplicationException e)
      {
        throw new RuntimeException( e);
      }

      GenericRecord record = new GenericData.Record( outputSchema);
      record.put( "encrypted_customer_account_id", encryptedCustomerAccountId);
      record.put( "encrypted_chc_id", encryptedChcId);
      record.put( "dtm_last_updated", (dtmLastUpdated != null) ? dtmLastUpdated.getTime( ) : null);
      record.put( "dtm_created", (dtmCreated != null) ? dtmCreated.getTime( ) : null);
      context.write( new AvroKey<GenericRecord>(record), NullWritable.get( ));
    }
  }

  public int run(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      System.err.println( "Usage: KomCustomerAccountBulkEncryptor -libjars ojdbc.jar <op_dir>");
      System.err.println( "<op_dir>: dir to store output files. this dir should not already exists");
      return -1;
    }

    String SELECT_QUERY = "SELECT customer_account_id, corp, LPAD(LTRIM(dwelling_nbr,' '),6,'0') dwelling_nbr, "
          + "LPAD(LTRIM(cust,' '),2,'0') cust, dtm_last_updated, dtm_created FROM cdrusr.kom_customer_account";

    String COUNT_QUERY = "SELECT COUNT(customer_account_id) FROM cdrusr.kom_customer_account";

    Schema outputSchema = KomCustomerAccount.getClassSchema( );
    Configuration conf = getConf( );
    conf.set( "mapred.map.tasks", "5");
    conf.set( "mapreduce.map.output.compress", "true");
    conf.set( "mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");
    conf.set( "mapreduce.output.fileoutputformat.compress", "true");
    conf.set( "mapreduce.output.fileoutputformat.compress.type", "block");
    conf.set( "mapreduce.output.fileoutputformat.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

    conf.set( OUTPUT_SCHEMA, outputSchema.toString( ));
    
    Map<String, String> map = Util.getSchema();
    String DRIVER_CLASS = map.get("DRIVER_CLASS");
    String DB_URL = map.get("DB_URL");
    String USER_NAME = map.get("USER_NAME");
    String PASSWORD = map.get("PASSWORD");
    
    logger.info( "DEBUG: DRIVER_CLASS = " + DRIVER_CLASS);
    logger.info( "DEBUG: DB_URL = " + DB_URL);
    logger.info( "DEBUG: USER_NAME = " + USER_NAME);
    logger.info( "DEBUG: PASSWORD = " + PASSWORD);
    
    DBConfiguration.configureDB( conf, DRIVER_CLASS, DB_URL, USER_NAME, PASSWORD);
    String configurationFile = System.getProperty("oozie.action.conf.xml");

    if (configurationFile != null)
    {
      conf.addResource(new Path("file:///", configurationFile));
    }

    Job job = Job.getInstance( conf);
    job.setJarByClass(KomCustomerAccountBulkEncryptor.class);
    job.setJobName("KomCustomerAccountBulkEncryptor");

    System.out.println( "DEBUG: SELECT QUERY = " + SELECT_QUERY);
    System.out.println( "DEBUG: COUNT QUERY = " + COUNT_QUERY);

    DBInputFormat.setInput( job, KomCustomerAccountDBInputWritable.class, SELECT_QUERY, COUNT_QUERY);
    FileOutputFormat.setOutputPath(job, new Path( args[args.length-1]));

    job.setMapOutputKeyClass( Text.class);
    job.setMapOutputValueClass( NullWritable.class);

    AvroJob.setOutputKeySchema( job, outputSchema);
    job.setOutputValueClass( NullWritable.class);
    job.setOutputFormatClass(AvroKeyOutputFormat.class);

    job.setMapperClass( MyMapper.class);
    job.setReducerClass( MyReducer.class);

    job.setNumReduceTasks(100);

    boolean status = job.waitForCompletion(true);
    cleanup( conf, args);
    return status ? 0 : 1;
  }

  private void cleanup( Configuration conf, String[] args)
      throws IOException
  {
    Util.deleteSuccessFiles( conf, args[args.length-1]);
  }

  public static void main(String[] args) throws Exception
  {
    int res =  ToolRunner.run(new Configuration(), new KomCustomerAccountBulkEncryptor(), args);
    System.exit(res);
  }
}
