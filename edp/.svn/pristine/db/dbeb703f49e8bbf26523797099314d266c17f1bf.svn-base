package com.cv.bis.security.utility;

import java.security.NoSuchAlgorithmException;
import java.security.spec.AlgorithmParameterSpec;
import java.security.spec.KeySpec;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.PBEParameterSpec;

import org.apache.commons.codec.binary.Base64;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;

public class CVSecurityOmnitureCryptoUtilBizz {

	Logger logger = LoggerFactory
			.getLogger(CVSecurityOmnitureCryptoUtilBizz.class);
	private byte[] salt = { (byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32,
			(byte) 0x56, (byte) 0x34, (byte) 0xE3, (byte) 0x03 };
	private String algName = "PBEWithMD5AndDES";
	private int iterationCount = 2;
	private Cipher ecipher;
	private Cipher dcipher;

	/**
	 * Default constructor
	 * 
	 * @param passPhrase
	 */
	public CVSecurityOmnitureCryptoUtilBizz(String passPhrase)
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
			KeySpec ks = new PBEKeySpec(passPhrase.toCharArray(), salt,
					iterationCount);
			SecretKey key = SecretKeyFactory.getInstance(algName)
					.generateSecret(ks);
			AlgorithmParameterSpec paramSpec = new PBEParameterSpec(salt,
					iterationCount);

			dcipher = Cipher.getInstance(key.getAlgorithm());
			ecipher = Cipher.getInstance(key.getAlgorithm());

			// Prepare the parameter to the ciphers
			// Create the ciphers
			ecipher.init(Cipher.ENCRYPT_MODE, key, paramSpec);
			dcipher.init(Cipher.DECRYPT_MODE, key, paramSpec);
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage());
		} catch (Exception exp) {
			logger.error("Error in  CVSecurityUtilityOmniture > " + passPhrase
					+ "::" + exp.getMessage());
		}

	}

	/**
	 * This method is to encrypt the given value
	 * 
	 * @param source
	 * @return
	 */
	public String encrypt(String source) {
		byte[] result = null;
		try {
			// Gets the raw bytes to encrypt, UTF8 is needed for
			// having a standard character set
			result = this.ecipher.doFinal(source.getBytes());
			String base64 = Base64.encodeBase64String(result);
			return base64;
		} catch (Exception e) {
			logger.error("Encryption at ::" + source + "::" + e.getMessage());
		}
		return "";
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
