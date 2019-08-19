package com.cv.bis.security.utility;

import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;

public final class CVSecurityOmnitureDecryptAcctNumber {

	Logger logger = LoggerFactory
			.getLogger(CVSecurityOmnitureDecryptAcctNumber.class);
	private Cipher dcipher;

	/**
	 * Default constructor
	 * 
	 * @param passPhrase
	 */
	public CVSecurityOmnitureDecryptAcctNumber(String passPhrase)
			throws CVApplicationException {
		init(passPhrase);
	}

	/**
	 * Initialization of security keys
	 * 
	 * @param passPhrase
	 * @throws Exception
	 */
	private void init(String passPhrase) throws CVApplicationException {
		try {
			if (null == passPhrase) {
				passPhrase = "cAb1ev!s!0n-c10ud";
			}
			// Iteration count
			SecretKeySpec skeySpec = new SecretKeySpec(
					passPhrase.getBytes("UTF-8"), "AES");
			dcipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
			dcipher.init(Cipher.DECRYPT_MODE, skeySpec);

		} catch (NoSuchAlgorithmException e) {

			logger.error(e.getMessage());
		} catch (Exception exp) {
			logger.error("Error in  CVSecurityUtilityOmniture > " + passPhrase
					+ "::" + exp.getMessage());
		}

	}

	/**
	 * This method is to decrypt the given encrypted value
	 * 
	 * @param encryptedData
	 * @return
	 */
	public String decrypt(String encryptedData) {
		String stringResult = null;
		try {

			byte[] result = dcipher.doFinal(Base64.decodeBase64(encryptedData));
			stringResult = new String(result, "UTF-8");
			return stringResult;
		} catch (Exception e) {
			logger.error("Decryption at::" + e.getMessage());
		}
		return "";
	}

}
