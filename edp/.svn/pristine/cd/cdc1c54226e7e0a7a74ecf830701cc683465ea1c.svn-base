package com.cablevision.edh.udf;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;


public class Channel900EncryptDecryptFunc implements EncryptDecryptFunc{
	
	private static final String SYSTEM_ID = "CHANNEL900";
	String user;
	static CVDataController cvDataController;
	
	public Channel900EncryptDecryptFunc(String user, String nameNodeService) throws CVApplicationException{
		this.user = user;
		cvDataController = new CVDataController(SYSTEM_ID, user, nameNodeService);
	}

	@Override
	public String decrypt(String fieldName, String encryptedText) throws CVApplicationException {
		return cvDataController.decryptField(fieldName, encryptedText).trim();
	}

	@Override
	public String encrypt(String fieldName, String value) throws CVApplicationException {
		return cvDataController.encryptField(fieldName, value);
	}

}
