package com.cv.bis.security.utility;


import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cv.bis.security.controller.CVDataController;
import com.cv.bis.security.exception.CVApplicationException;
import com.cv.bis.security.exception.CVSecurityException;




/**
 * This class helps to encrypt and decrypt given values
 * 
 * @author Subramanian Karuppiah
 *
 */
public final class CVSecurityUtility {
	
	Logger logger = LoggerFactory.getLogger(CVSecurityUtility.class);
    private static CVSecurityUtility CIPHER = null;
    private static final Object lock = new Object();
    private Cipher ecipher;
    private Cipher dcipher;
    CVSecurityUtility cvc = null;

    // 8-byte Salt
    byte[] salt = {0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19, 0x19};
    byte[] saltx = {(byte) 0xA9, (byte) 0x9B, (byte) 0xC8, (byte) 0x32, (byte) 0x56, (byte) 0x35, (byte) 0xE3, (byte) 0x03};
    byte[] iv ={
    		0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0
    		};
    

    /**
     * Default constructor
     * @param passPhrase
     */
    public CVSecurityUtility(String passPhrase) throws CVApplicationException{
            init(passPhrase);
    }

    /**
     * Initialization of security keys
     * @param passPhrase
     * @throws Exception
     */
    private void init(String passPhrase) throws CVApplicationException {
        try {
            if (null == passPhrase) {
                passPhrase = "cAb1ev!s!0n-c10ud";
            }
            
                // Iteration count
	        IvParameterSpec ivspec = new IvParameterSpec(iv);
	        SecretKeySpec skeySpec = new SecretKeySpec(passPhrase.getBytes(), "AES");
	        ecipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
	        dcipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
	        
	        // Prepare the parameter to the ciphers
	        // Create the ciphers
	        
	        ecipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivspec);
	        dcipher.init(Cipher.DECRYPT_MODE, skeySpec, ivspec);
         
        } catch (NoSuchAlgorithmException e){
        	logger.error(e.getMessage());
        }catch (Exception exp) {
        	logger.error("Error in CVSecurityUtility > " + passPhrase + "::" + exp.getMessage());
        }

    }

    /**
     * Initialization
     * @param key
     * @return
     * @throws Exception
     */
   /* public static final CVSecurityUtility getInstance(String key) throws CVApplicationException{
    	synchronized (lock) {
              CIPHER = new CVSecurityUtility(key);
        
		}
        return CIPHER;
    }
*/
    /**
     * This method is to encrypt the given value
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
        	logger.error("Encryption at ::"+source +"::"+e.getMessage());
        }
        return "";
    }

    /**
     * This method is to decrypt the given encrypted value
     * @param encryptedData
     * @return
     */
    public String decrypt(String encryptedData) {
        String stringResult = null;
        try {
            byte[] dec = fromHexString(encryptedData);
            byte[] result = dcipher.doFinal(dec);
            stringResult = new String(result);
            return stringResult;
        } catch (Exception e) {
            logger.error("Decryption at::"+e.getMessage());
        }
        return "";
    }

    
   /**
    * 
    * @param securityKey
    * @return
    */
	public String getPGPDecryptKey(String securityKey,String pgbKey) {
		
		String stringResult = null;
        try {
            byte[] dec = Base64.decodeBase64(securityKey.getBytes());
           byte[] result = dcipher.doFinal(dec);
            stringResult = new String(result);
            return stringResult;
        } catch (Exception e) {
        	try {
				cvc = new CVSecurityUtility(pgbKey);
				stringResult = cvc.getPGPDecryptKey(securityKey,"");
				return stringResult;
			} catch (CVApplicationException e1) {
				return "";
			}
        }
	}
	
	/**
	 * Convert bytes to hexdecimal value
	 * @param a
	 * @return
	 */
	private String byteToHexDec(byte[] array) {
  	   StringBuilder sb = new StringBuilder(array.length * 2);
  	   for(byte b: array)
  	      sb.append(String.format("%02X", b & 0xff));
  	   return sb.toString();
  	}
	 
	
	/**
	 * Convert to byte from hexdecimal
	 * @param s
	 * @return
	 */
	public static byte[] fromHexString(String s) {
        int len = s.length();
        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                                 + Character.digit(s.charAt(i+1), 16));
        }
        return data;
	}
	
	
}
