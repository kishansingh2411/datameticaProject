package com.cablevision.edp.utility;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.tool.ConcatTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergeHdfsSinkFiles {

	private static Logger logger = LoggerFactory.getLogger(MergeHdfsSinkFiles.class);
	public static void main(String[] args) {

		try {		
			Configuration conf = new Configuration();

			FileSystem fs = FileSystem.get(conf);
			logger.info("fs URI "+fs.getUri());
			List<String> fileNameList = new ArrayList<String>();
			FileStatus[] fileList = fs.listStatus(new Path("hdfs://cvlhdpdev/user/etlmgr/e2e/custmaster/load_year=2013/load_month=12/load_day=20/temp/secure_edp_custmaster"));
			logger.info("number of hdfs files "+fileList.length);
			
			for(FileStatus fileStatus: fileList)
			{
				logger.info("file name found "+fileStatus.getPath().getName());
				fileNameList.add(fileStatus.getPath().getName());
			}
			
			ConcatTool concatTool = new ConcatTool();
			File outputFile = new File("/home/etlmgr/secure_edp_custmaster_output.avro");
			
			OutputStream out = new FileOutputStream(outputFile);
			BufferedOutputStream outStream = new BufferedOutputStream(out);
			
			concatTool.run(null, new PrintStream(outStream),  
									new PrintStream(System.err), fileNameList);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
