package com.cablevision.edh.udf;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;


public class MRDVREncryptDecryptFunc implements EncryptDecryptFunc{
	
	private static final String SYSTEM_ID = "MRDVR";
	String user;
	static CVDataController cvDataController;
	
	public MRDVREncryptDecryptFunc(String user, String nameNodeService) throws CVApplicationException{
		this.user = user;
		cvDataController = new CVDataController(SYSTEM_ID, user, nameNodeService);
	}

	public String decrypt(String fieldName, String encryptedText) throws CVApplicationException {
		return cvDataController.decryptField(fieldName, encryptedText).trim();
	}

	public String encrypt(String fieldName, String value) throws CVApplicationException {
		return cvDataController.encryptField(fieldName, value);
	}
	
	
	public static void main(String[] args) {
		try {
			String encryptVal = cvDataController.encryptField("HOME_ID", "9335118");
			System.out.println("encryptVal......"+encryptVal);
		} catch (CVApplicationException e) {
			e.printStackTrace();
		}

	}

}
