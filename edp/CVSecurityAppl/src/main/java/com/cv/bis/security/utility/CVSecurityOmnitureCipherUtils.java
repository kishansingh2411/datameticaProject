package com.cv.bis.security.utility;

import java.security.NoSuchAlgorithmException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.exception.CVApplicationException;

public class CVSecurityOmnitureCipherUtils {
	Logger logger = LoggerFactory
			.getLogger(CVSecurityOmnitureCipherUtils.class);
	private Cipher dcipher;
	private Cipher ecipher;
	private final static String ALGORITHM_shortname = "DESede";
	private final static String ALGORITHM_fullname = "DESede/ECB/PKCS5Padding";
	private byte[] keyBytes = new byte[24];

	// private final static String pass = "thisIsTotallySecure";

	/**
	 * Default constructor
	 * 
	 * @param passPhrase
	 */
	public CVSecurityOmnitureCipherUtils(String passPhrase)
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
			System.arraycopy(passPhrase.getBytes(), 0, keyBytes, 0,
					passPhrase.getBytes().length);
			SecretKey ks = new SecretKeySpec(keyBytes, ALGORITHM_shortname);
			dcipher = Cipher.getInstance(ALGORITHM_fullname);
			ecipher = Cipher.getInstance(ALGORITHM_fullname);
			ecipher.init(Cipher.ENCRYPT_MODE, ks);
			dcipher.init(Cipher.DECRYPT_MODE, ks);
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage());
		} catch (Exception exp) {
			logger.error("Error in CVSecurityOmnitureCryptoUtils > "
					+ passPhrase + "::" + exp.getMessage());
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
			byte[] dec = fromHexString(encryptedData);
			byte[] result = dcipher.doFinal(dec);
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
		byte[] btemp = new byte[1024];
		try {
			btemp = source.getBytes("UTF-8");
			result = this.ecipher.doFinal(btemp);
			return new String(byteToHexDec(result));
		} catch (Exception e) {
			logger.error("Encryption at ::" + source + "::" + e.getMessage());
		}
		return "";
	}

	/**
	 * Convert to byte from hexdecimal
	 * 
	 * @param s
	 * @return
	 */
	public static byte[] fromHexString(String s) {
		int len = s.length();
		byte[] data = new byte[len / 2];
		for (int i = 0; i < len; i += 2) {
			data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4) + Character
					.digit(s.charAt(i + 1), 16));
		}
		return data;
	}

	/**
	 * Convert bytes to hexdecimal value
	 * 
	 * @param a
	 * @return
	 */
	private String byteToHexDec(byte[] array) {
		StringBuilder sb = new StringBuilder(array.length * 2);
		for (byte b : array)
			sb.append(String.format("%02X", b & 0xff));
		return sb.toString();
	}



}
