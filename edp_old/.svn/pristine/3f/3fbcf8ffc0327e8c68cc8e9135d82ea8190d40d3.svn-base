package com.cv.bis.security.dao;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;
import com.cv.bis.security.exception.CVSecurityException;
import com.cv.bis.security.utility.CVPropertiesUtility;

public class CVSecurityDAOImpl implements CVSecurityDAO {

	Logger logger = LoggerFactory.getLogger(CVSecurityDAOImpl.class);

	/**
	 * load db values
	 */
	public HashMap<String, String> getKeyMap(String userName, String password, String dbUrl, String sysId, String driverName) throws CVApplicationException {

		//CVPropertiesUtility propsUtil = new CVPropertiesUtility();

		Connection conn = null;
		Statement stmt = null;
		ResultSet rs = null;
		String passKey = "";
		String fieldName = "";

		HashMap<String, String> dbMap = new HashMap<String, String>();
		try {

			Class.forName(driverName);
			//Class.forName("oracle.jdbc.driver.OracleDriver");
    		Properties props = new Properties();
			props.setProperty("user", userName);
			props.setProperty("password", password);
			conn = DriverManager.getConnection(dbUrl, props);
			stmt = conn.createStatement();
			String sql = "";
			sql = "Select passKey,fieldName from edp_cv_security_master where systemId='" + sysId + "'";
		    rs = stmt.executeQuery(sql);
		    while (rs.next()) {
				passKey = rs.getString(1);
				fieldName = rs.getString(2).toLowerCase();
				dbMap.put(fieldName, passKey);			 
			}
		} catch (SQLException e) {
			e.printStackTrace();
			logger.error(e.getMessage());
			throw new CVSecurityException(CVSecurityException.SECURITY_KEY_DB_GET_ERROR);
		} catch (ClassNotFoundException e) {
			logger.warn(e.getMessage());
			throw new CVSecurityException(CVSecurityException.SECURITY_KEY_DRIVER_ERROR);
		} finally {
			try {
				if (null != rs)
					rs.close();
				if (null != stmt)
					stmt.close();
				if (null != conn)
					conn.close();
			} catch (Exception ex) {
				logger.warn(ex.getMessage());
			}
		}
		return dbMap;
	}
	
	

}
