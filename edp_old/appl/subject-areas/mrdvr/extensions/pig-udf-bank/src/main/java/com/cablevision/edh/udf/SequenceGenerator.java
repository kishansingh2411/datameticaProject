package com.cablevision.edh.udf;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SequenceGenerator extends EvalFunc<Long> {

	private static final Logger logger = LoggerFactory.getLogger(SequenceGenerator.class);
	private static Long next_seq_gen = 0L;
	private static Connection conn = null;
	Map<String, String> propsMap = new HashMap<String, String>();

	@Override
	public Long exec(Tuple input) throws IOException {

		if (input == null || input.size() == 0) {
			logger.error("input is null");
			return null;
		}

		try {
			String tableName = input.get(0).toString();
			String nameNodeService = input.get(1).toString();

			if (conn == null) {
				logger.info("Creating connection.");
				loadDBPropertiesFromFile(nameNodeService.toString());
	
				String host = (String) propsMap.get("POSTGRES_HOST");
				String port = (String) propsMap.get("POSTGRES_PORT");
				String database = (String) propsMap.get("POSTGRES_DATABASE_NAME");
				String username = (String) propsMap.get("POSTGRES_USERNAME");
				String password = (String) propsMap.get("POSTGRES_PASSWORD");

				logger.info("Creating DB instance.");
				DriverManager.registerDriver(new org.postgresql.Driver());
				conn = DriverManager.getConnection("jdbc:postgresql://" + host + ":" + port + "/" + database, username,
						password);
				conn.setAutoCommit(false);
			}
			
			PreparedStatement ps = conn.prepareStatement("select nextval('" + tableName + "_seq') as num;");
			ResultSet rs = ps.executeQuery();
			
			while (rs.next()) {
				next_seq_gen = rs.getLong("num");
			}
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}

		return next_seq_gen;
	}

	private void loadDBPropertiesFromFile(String nameNodeService) throws Exception {
		try {
			propsMap = new LoadPropertiesUtility(nameNodeService).getDBProperties();
			if (propsMap.isEmpty()) {
				throw new Exception("Error occured while loading the properties files");
			}
		} catch (Exception e) {
			logger.warn(e.getMessage());
			throw e;
		}
	}

	@Override
	public void finish() {
		try {
			logger.info("Closed Database Successfully");
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			e.printStackTrace();
			System.exit(0);
		}
	}
}
