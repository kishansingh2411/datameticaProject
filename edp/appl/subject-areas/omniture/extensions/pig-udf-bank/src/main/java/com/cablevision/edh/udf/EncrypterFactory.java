package com.cablevision.edh.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;

public class EncrypterFactory {

	private static final Logger logger = LoggerFactory.getLogger(EncrypterFactory.class);

	private String user;
	
	private String nameNodeService;

	private EncryptDecryptFunc onetAccNumEncryptDecryptFunc;

	private EncryptDecryptFunc onetOptimumIdEncryptDecryptFunc;

	private EncryptDecryptFunc vowEncryptDecryptFunc;
	
	private EncryptDecryptFunc globalUnifiedEncryptDecryptFunc;
	
	private EncryptDecryptFunc channel900EncryptDecryptFunc;

	public EncrypterFactory(String user, String nameNodeService) {
		this.user = user;
		this.nameNodeService = nameNodeService;
	}

	public EncryptDecryptFunc getEncryptDecryptFunc(String suiteName, String fieldName) throws EncrypterInstantiationException {
		if (suiteName.equalsIgnoreCase("onet_prod") || suiteName.equalsIgnoreCase("onet_prod_esp")) {
			if (fieldName.equalsIgnoreCase("TTT_ACCT_NUMBER")) {
				try {
					if (onetAccNumEncryptDecryptFunc == null) {
						onetAccNumEncryptDecryptFunc = new OnetAccNumEncryptDecryptFunc(user, nameNodeService);
					}
					return onetAccNumEncryptDecryptFunc;
				} catch (CVApplicationException e) {
					logger.error(e.getMessage(), e);
					throw new EncrypterInstantiationException(e.getMessage());
				}
			} else if (fieldName.equalsIgnoreCase("OPTIMUM_ID")) {
				try {
					if (onetOptimumIdEncryptDecryptFunc == null) {
						onetOptimumIdEncryptDecryptFunc = new OnetOptimumIdEncryptDecryptFunc(user, nameNodeService);
					}
					return onetOptimumIdEncryptDecryptFunc;
				} catch (CVApplicationException e) {
					logger.error(e.getMessage(), e);
					throw new EncrypterInstantiationException(e.getMessage());
				}
			}
		} else if (suiteName.equalsIgnoreCase("vow") || suiteName.equalsIgnoreCase("vow_esp")) {
			try {
				if (vowEncryptDecryptFunc == null) {
					vowEncryptDecryptFunc = new VowEncryptDecryptFunc(user, nameNodeService);
				}
				return vowEncryptDecryptFunc;
			} catch (CVApplicationException e) {
				logger.error(e.getMessage(), e);
				throw new EncrypterInstantiationException(e.getMessage());
			}
		} else if (suiteName.equalsIgnoreCase("global_unified")) {
			try {
				if (globalUnifiedEncryptDecryptFunc == null) {
					globalUnifiedEncryptDecryptFunc = new GlobalUnifiedEncryptDecryptFunc(user, nameNodeService);
				}
				return globalUnifiedEncryptDecryptFunc;
			} catch (CVApplicationException e) {
				logger.error(e.getMessage(), e);
				throw new EncrypterInstantiationException(e.getMessage());
			}
		} else if (suiteName.equalsIgnoreCase("CHANNEL900")) {
			try {
				if (channel900EncryptDecryptFunc == null) {
					channel900EncryptDecryptFunc = new Channel900EncryptDecryptFunc(user, nameNodeService);
				}
				return channel900EncryptDecryptFunc;
			} catch (CVApplicationException e) {
				logger.error(e.getMessage(), e);
				throw new EncrypterInstantiationException(e.getMessage());
			}
		}
		return null;
	}

}
