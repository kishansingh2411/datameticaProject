package com.cablevision.edh.udf;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoadPropertiesUtility {

	Logger logger = LoggerFactory.getLogger(LoadPropertiesUtility.class);
	Properties prop;
	InputStream input = null;
	String nameNodeService;
	
	public LoadPropertiesUtility(){
	}
	
	public LoadPropertiesUtility(String nameNodeUrl){
		this.nameNodeService = nameNodeUrl;
	}
	
	/**
	 * Get properties
	 * 
	 * @return
	 */
	public Map<String, String> getDBProperties() throws Exception {
		Map<String, String> propertiesMap = new HashMap<String, String>();
		Map<String, String> dbPropertiesMap = null;
		try {
			prop = new Properties();
			//String propsFileName = "conf/security-schema.properties";
			String propsFileName = "conf/config.properties";
			input = getClass().getClassLoader().getResourceAsStream(propsFileName);
			if (input == null) {
				logger.warn("Unable to find file ->" + propsFileName);
			}
			prop.load(input);

			Enumeration<?> e = prop.propertyNames();
			while (e.hasMoreElements()) {
				String key = (String) e.nextElement();
				String value = prop.getProperty(key);
				propertiesMap.put(key, value);
			}
			
			String filePath = propertiesMap.get("schema.location");
			if(filePath!=null && !filePath.equals("")){
				logger.info("Found file path as : "+filePath);
				dbPropertiesMap = getSchemaDetails(filePath);
			}
		} catch (Exception ex) {
			logger.error(ex.getMessage(),ex);
		}
		return dbPropertiesMap;
	}

	private Map<String, String> getSchemaDetails(String schemaFilePath) throws Exception {
		Map<String, String> dbPropertiesMap = new HashMap<String, String>();
		String currentLine = null;
		String[] tokens = null;
		BufferedReader br =null;
		try{
			Configuration conf = new Configuration();
			URI uri = URI.create("hdfs://"+nameNodeService+"/"+schemaFilePath+"/postgres.properties");
	        FileSystem fs = FileSystem.get(uri, conf);
	        Path path  = new Path(uri);
	        if(fs.exists(path)){
		        br = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
		        while ((currentLine = br.readLine()) != null) {
		        	if(currentLine.contains("=")){
			        	tokens = currentLine.split("=");
			        	dbPropertiesMap.put(tokens[0], currentLine.replace(tokens[0]+"=", ""));
		        	}
				}
	        }else{
	        	logger.error("Schema Details file not found ->" + path.getName());
	        }
		}catch(Exception ex){
			logger.error(ex.getMessage(),ex);
		} finally{
        	try {
				br.close();
			} catch (IOException ex) {
				logger.error(ex.getMessage(),ex);
			}
        }
		return dbPropertiesMap;
	}

}
