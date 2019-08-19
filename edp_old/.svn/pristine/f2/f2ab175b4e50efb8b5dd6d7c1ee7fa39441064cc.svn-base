package com.cv.bis.security.controller;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.dao.CVSecurityDAO;
import com.cv.bis.security.dao.CVSecurityDAOImpl;
import com.cv.bis.security.exception.CVApplicationException;
import com.cv.bis.security.exception.CVSecurityException;
import com.cv.bis.security.utility.CVPropertiesUtility;
import com.cv.bis.security.utility.CVSecurityChannel900;
import com.cv.bis.security.utility.CVSecurityOmnitureCipherUtils;
import com.cv.bis.security.utility.CVSecurityUtility;
import com.cv.bis.security.utility.CVSecurityOmnitureDecryptAcctNumber;
import com.cv.bis.security.utility.CVSecurityOmnitureCryptoUtil;
import com.cv.bis.security.utility.CVSecurityOmnitureCryptoUtilBizz;

/**
 * This controller is the gateway for encryption / decryption. The pass keys for
 * encryption/decryption are looked up in the following order.
 * <ul>
 * <li>Redshift Database</li>
 * <li>Running Environment (shell under which calling application executes)</li>
 * <li>Properties file</li>
 * </ul>
 * <p>
 * Calling application will pass system id and user who executes it.
 * </p>
 * 
 * @author Q4
 * 
 */
public final class CVDataController {

	Logger logger = LoggerFactory.getLogger(CVDataController.class);
	HashMap<String, String> secMap = new HashMap<String, String>();
	Map<String, String> propsMap = new HashMap<String, String>();
	Map<String, CVSecurityUtility> cipherMap = new HashMap<String, CVSecurityUtility>();
	String dbEnvUserName = "";
	String dbEnvPassword = "";
	String dbEnvUrl = "";
	String pgpKey = "";
	String principal = null;
	String systemId = null;
	String envDriverName = "";
	String envPGBKey = "";
	String nameNodeService = "";

	HashMap<String, String> secureKeyMap = null;

	public CVDataController(String systemId, String who, String nameNodeUrl) throws CVApplicationException {
		this.systemId = systemId;
		this.principal = who;
		this.nameNodeService = nameNodeUrl;
		init();
	}

	/**
	 * 
	 * @throws CVApplicationException
	 */
	private void init() throws CVApplicationException {
		try {
			verifySecurity();
			loadDBPropertiesFromFile();
			loadDBPropsFromEnv();
			loadSecurityMapFromDB();
		} catch (Exception e) {
			logger.error(e.getMessage());
			throw e;
		}
	}

	/**
	 * To get encrypted field
	 * 
	 * @param fieldName
	 * @param data
	 * @return
	 * @throws CVApplicationException
	 */
	public String encryptField(String fieldName, String data) throws CVApplicationException {

		CVSecurityUtility cvc = null;
		String secKey = getSecurityKey(fieldName);
    	String result = "";
    	secKey = (secKey == null) ? "" : secKey;
		if (secKey == null || secKey.length() == 0) {
			result = "";
		} else {
			if (!cipherMap.containsKey(secKey)) {
				cvc = new CVSecurityUtility(secKey);
				cipherMap.put(secKey, cvc);
				result = cvc.encrypt(data);
			} else {
				result = cipherMap.get(secKey).encrypt(data);
			}
		}
		return result;
	}

	/**
	 * To get decrypted field
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String decryptField(String fieldName, String data)
			throws CVApplicationException {

		CVSecurityUtility cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityUtility(getSecKey);
			if (!isNullOrBlank(data)) {
				if (isAlphanumeric(data))
					toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}

	/**
	 * Get the security key
	 * 
	 * @param grpId
	 * @return
	 * @throws CVApplicationException
	 */
	private String getSecurityKey(String fieldName) throws CVApplicationException {
		try {

			String securityKey = "";

			securityKey = (String) secureKeyMap.get(fieldName.toLowerCase());
			securityKey = (securityKey == null) ? "" : securityKey;

			logger.info("Fetching security key from the map");
			String origKey = secureKeyMap.get(securityKey);

			if (origKey == null) {
				logger.info("Security key not found in the Map, decrypting the key");
				origKey = getPGPDecryptKey(securityKey);
				secureKeyMap.put(securityKey, origKey);
			}

			return origKey;

		} catch (CVApplicationException e) {
			logger.warn(e.getMessage());
		}

		return "";
	}


	/**
	 * 
	 * @param securityKey
	 * @return
	 * @throws CVApplicationException
	 */
	private String getPGPDecryptKey(String securityKey)
			throws CVApplicationException {
		CVSecurityUtility cvc = null;
		if (pgpKey == null)
			pgpKey = (String) propsMap.get("security.pgb.key");
		pgpKey = (null == pgpKey) ? "" : pgpKey;

		if (envPGBKey != null && envPGBKey.length() > 1) {
			pgpKey = envPGBKey;
		} else {
			pgpKey = new CVPropertiesUtility(nameNodeService).getDBProperties().get(
					"security.pgb.key");
		}
		String result = "";

		if (!cipherMap.containsKey(pgpKey)) {
			cvc = new CVSecurityUtility(pgpKey);
			cipherMap.put(pgpKey, cvc);
			result = cvc.getPGPDecryptKey(securityKey, pgpKey);
		} else {
			result = cipherMap.get(pgpKey)
					.getPGPDecryptKey(securityKey, pgpKey);
		}

		return result;
	}

	/**
	 * Load properties
	 * 
	 * @throws CVApplicationException
	 */
	private void loadDBPropertiesFromFile() throws CVApplicationException {
		try {
			propsMap = new CVPropertiesUtility(nameNodeService).getDBProperties();
			if(propsMap.isEmpty()){
				throw new CVApplicationException("Error occured while loading the properties files");
			}
		} catch (CVApplicationException e) {
			logger.warn(e.getMessage());
			throw e;
		}
	}

	/**
	 * Load System Env variables
	 */
	private void loadDBPropsFromEnv() {

		Map<String, String> env = System.getenv();
		if (env.containsKey("ORACLE_USERNAME"))
			dbEnvUserName = env.get("ORACLE_USERNAME");
		if (env.containsKey("ORACLE_PASSWORD"))
			dbEnvPassword = System.getenv("ORACLE_PASSWORD");
		if (env.containsKey("ORACLE_DB_URL"))
			dbEnvUrl = System.getenv("ORACLE_DB_URL");
		if (env.containsKey("security.pgpkey"))
			pgpKey = System.getenv("security.pgpkey");
		if (env.containsKey("DRIVER_NAME"))
			envDriverName = System.getenv("DRIVER_NAME");
		if (env.containsKey("PGB_KEY"))
			envPGBKey = System.getenv("PGB_KEY");

	}

	/**
	 * Load db values
	 */
	private void loadSecurityMapFromDB() throws CVApplicationException {
		CVSecurityDAO securityDao = new CVSecurityDAOImpl();
		String userName = "";
		String pwd = "";
		String dburl = "";
		String driverName = "";

		String url = (String) propsMap.get("redshift.db.url");

		if (dbEnvUserName != null && dbEnvUserName.length() > 1) {
			userName = dbEnvUserName;
		} else {
			userName = (String) propsMap.get("redshift.database.username");
		}
		if (dbEnvPassword != null && dbEnvPassword.length() > 1) {
			pwd = dbEnvPassword;
		} else {
			pwd = (String) propsMap.get("redshift.database.password");
		}
		if (dbEnvUrl != null && dbEnvUrl.length() > 1) {
			dburl = dbEnvUrl;
		} else {
			dburl = (String) propsMap.get("redshift.db.url");
		}
		if (envDriverName != null && envDriverName.length() > 1) {
			driverName = envDriverName;
		} else {
			driverName = (String) propsMap.get("redshift.database.driver");
		}

		userName = (userName == null) ? "" : userName;
		pwd = (pwd == null) ? "" : pwd;

		secureKeyMap = securityDao.getKeyMap(userName, pwd, dburl, systemId,
				driverName);

	}

	/**
	 * Verify if the user running this utility is who they claims to be
	 */
	private void verifySecurity() throws CVApplicationException {

		Map<String, String> env = System.getenv();

		if (env.containsKey("USER")) {
			if (!this.principal.equals(env.get("USER")))
				throw new CVSecurityException(CVSecurityException.UNKNOWN_USER);
		}
		if (env.containsKey("USERNAME")) {
			if (!this.principal.equals(env.get("USERNAME")))
				throw new CVSecurityException(CVSecurityException.UNKNOWN_USER);
		}

	}

	/**
	 * 
	 * @param str
	 * @return
	 */
	private boolean isAlphanumeric(String str) {
		for (int i = 0; i < str.length(); i++) {
			char c = str.charAt(i);
			if (c < 0x30 || (c >= 0x3a && c <= 0x40) || (c > 0x5a && c <= 0x60)
					|| c > 0x7a)
				return false;
		}

		return true;
	}

	/**
	 * 
	 * @param s
	 * @return
	 */
	private static boolean isNullOrBlank(String s) {
		return (s == null || s.trim().equals(""));
	}

	/**
	 * To decryptOmnitureAccount Number
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String decryptAcctNumber(String fieldName, String data)
			throws CVApplicationException {
		CVSecurityOmnitureDecryptAcctNumber cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityOmnitureDecryptAcctNumber(getSecKey);
			if (!isNullOrBlank(data)) {
				// if(isAlphanumeric(data))
				toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}

	/**
	 * To decryptOmnitureCryptoUtil
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String decryptOmnitureCryptoUtil(String fieldName, String data)
			throws CVApplicationException {
		CVSecurityOmnitureCryptoUtil cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityOmnitureCryptoUtil(getSecKey);
			if (!isNullOrBlank(data)) {
				// if(isAlphanumeric(data))
				toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}

	/**
	 * To decryptOmnitureCryptoUtilBizz
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String decryptOmnitureCryptoUtilBizz(String fieldName, String data)
			throws CVApplicationException {
		CVSecurityOmnitureCryptoUtilBizz cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityOmnitureCryptoUtilBizz(getSecKey);
			if (!isNullOrBlank(data)) {
				// if(isAlphanumeric(data))
				toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}

	/**
	 * To decryptOmnitureCryptoUtilBizz
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String decryptOmnitureCipherUtils(String fieldName, String data)
			throws CVApplicationException {
		CVSecurityOmnitureCipherUtils cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityOmnitureCipherUtils(getSecKey);
			if (!isNullOrBlank(data)) {
				// if(isAlphanumeric(data))
				toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}
	
	/**
	 * To decryptOmnitureCryptoUtilBizz
	 * 
	 * @param groupId
	 * @param source
	 * @return
	 * @throws CVApplicationException
	 */
	public String deObfuscateChannel900(String fieldName, String data)
			throws CVApplicationException {
		CVSecurityChannel900 cvc = null;
		String toDecrypt = "";
		String getSecKey = getSecurityKey(fieldName);
		if (getSecKey == null || getSecKey.length() == 0)
			toDecrypt = "";
		else {
			cvc = new CVSecurityChannel900(getSecKey);
			if (!isNullOrBlank(data)) {
				// if(isAlphanumeric(data))
				toDecrypt = cvc.decrypt(data);
			}
		}
		return toDecrypt;
	}
	
	

}
