package com.cablevision.edh.udf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;

public class EncrypterFactory {

	private static final Logger logger = LoggerFactory.getLogger(EncrypterFactory.class);

	private String user;
	
	private String nameNodeService;

	private EncryptDecryptFunc MRDVREncryptDecryptFunc;

	public EncrypterFactory(String user, String nameNodeService) {
		this.user = user;
		this.nameNodeService = nameNodeService;
	}

	public EncryptDecryptFunc getEncryptDecryptFunc(String suiteName, String fieldName) throws EncrypterInstantiationException {
	 if (suiteName.equalsIgnoreCase("MRDVR")) {
			try {
				if (MRDVREncryptDecryptFunc == null) {
					MRDVREncryptDecryptFunc = new MRDVREncryptDecryptFunc(user, nameNodeService);
				}
				return MRDVREncryptDecryptFunc;
			} catch (CVApplicationException e) {
				logger.error(e.getMessage(), e);
				throw new EncrypterInstantiationException(e.getMessage());
			}
		}
		
		return null;
	}

}
