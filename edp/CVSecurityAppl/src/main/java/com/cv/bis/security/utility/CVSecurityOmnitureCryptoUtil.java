package com.cv.bis.security.utility;

import java.security.NoSuchAlgorithmException;
import java.security.spec.KeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.Cipher;
import javax.crypto.spec.DESedeKeySpec;
import javax.crypto.SecretKeyFactory;

import org.apache.commons.codec.binary.Base64;
import com.cv.bis.security.exception.CVApplicationException;

public class CVSecurityOmnitureCryptoUtil {

	Logger logger = LoggerFactory.getLogger(CVSecurityOmnitureCryptoUtil.class);
	private Cipher dcipher;
	private Cipher ecipher;
	byte[] encKey = { (byte) 0x98, (byte) 0xbc, (byte) 0xdf, (byte) 0x64,
			(byte) 0x2c, (byte) 0x91, (byte) 0x13, (byte) 0x45, (byte) 0x97,
			(byte) 0x2c, (byte) 0x6e, (byte) 0x62, (byte) 0x10, (byte) 0x76,
			(byte) 0xa1, (byte) 0x51, (byte) 0x98, (byte) 0xbc, (byte) 0xdf,
			(byte) 0x64, (byte) 0x2c, (byte) 0x91, (byte) 0x13, (byte) 0x45 };

	/**
	 * Default constructor
	 * 
	 * @param passPhrase
	 */
	public CVSecurityOmnitureCryptoUtil(String passPhrase)
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

			KeySpec ks = new DESedeKeySpec(encKey);
			SecretKeyFactory kf = SecretKeyFactory.getInstance("DESede");

			dcipher = Cipher.getInstance("DESede");
			ecipher = Cipher.getInstance("DESede");
			// Prepare the parameter to the ciphers
			// Create the ciphers
			ecipher.init(Cipher.ENCRYPT_MODE, kf.generateSecret(ks));
			dcipher.init(Cipher.DECRYPT_MODE, kf.generateSecret(ks));
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
			System.out.println(e.getMessage());
			logger.error("Decryption at::" + e.getMessage());
		}
		return "";
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

}
