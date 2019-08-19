package com.cablevision.edh.udf;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;

public class OnetOptimumIdEncryptDecryptFunc implements EncryptDecryptFunc {
	
	private static final String SYSTEM_ID = "ONET";
	String user;
	CVDataController cvDataController;
//	private static final String FIELD_NAME = "OPTIMUM_ID";
	
	public OnetOptimumIdEncryptDecryptFunc(String user, String nameNodeService) throws CVApplicationException{
		this.user = user;
		cvDataController = new CVDataController(SYSTEM_ID, user, nameNodeService);
	}

	@Override
	public String decrypt(String fieldName, String encryptedText) throws CVApplicationException {
		return cvDataController.decryptOmnitureCryptoUtil(fieldName, encryptedText).trim();
	}

	@Override
	public String encrypt(String fieldName, String value) throws CVApplicationException {
		return cvDataController.encryptField(fieldName, value);
	}

}
