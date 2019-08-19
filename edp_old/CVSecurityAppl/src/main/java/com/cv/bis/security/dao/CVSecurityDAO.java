package com.cv.bis.security.dao;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.cv.bis.security.exception.CVApplicationException;

public interface CVSecurityDAO {
	
	public HashMap<String, String> getKeyMap(String username, String password, String dbUrl, String sysId, String driverName) throws CVApplicationException;
	
}
