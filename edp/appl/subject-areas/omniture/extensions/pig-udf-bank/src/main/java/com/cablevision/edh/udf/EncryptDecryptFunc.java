package com.cablevision.edh.udf;


import com.cv.bis.security.exception.CVApplicationException;

public interface EncryptDecryptFunc {
	
	public String decrypt(String fieldName, String encryptedText) throws CVApplicationException;
	
	public String encrypt(String fieldName, String text) throws CVApplicationException;

}
