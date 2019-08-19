package com.cv.bis.security.utility;

/*
//This class is a conversion of javascript 

*/
import java.io.UnsupportedEncodingException;
//import java.util.Base64;
import org.apache.commons.codec.binary.Base64;

import com.cv.bis.security.exception.CVApplicationException;

public class CVSecurityChannel900 {
	

	private final String OBSF_VER = "02";
	
    
	public  CVSecurityChannel900 (String passPhrase) throws CVApplicationException {
		}
	
	private int rotateRight(int value, int positions){
		      return (((value & 0xFF) >>> positions) | (((value & 0xFF) << (32 - positions)) >>> 24));
	  }
	
	
	private int clampSbyte (int characterCode){
		if(characterCode >= 128){
			characterCode -= 256;
		}
		return characterCode;
	}
	
	private int rotateLeft (int value, int positions){
		int rotated = (((value & 0xFF) << positions) | (((value & 0xFF) << positions) >> 8));
		return rotated;
	}
	
	private String _encrypt(String data) {
		String obstr = "";
		int len = data.length();
		
		for (int i=0; i < len; i++){
			int characterCode = data.codePointAt(i);
			characterCode ^= 0x49;
			characterCode = clampSbyte(characterCode);
			
			characterCode = rotateLeft(characterCode, 1);
			characterCode = clampSbyte(characterCode);
			
			characterCode ^= 0xD4;
			characterCode = clampSbyte(characterCode);
	         
			characterCode = rotateLeft(characterCode, 1);
			characterCode = clampSbyte(characterCode);
			
			obstr += "" + Character.toString((char)characterCode);
		}
		return obstr;
	}
	
	private String _decrypt(String decodedString){
		String deobstr = "";
		int len = decodedString.length();
		
		for (int i = 0; i < len; i++) {
	         int c = decodedString.codePointAt(i);
	         
	         c = rotateRight(c, 1);
	         c = clampSbyte(c);
	         
	         c ^= 0xD4;
	         c = clampSbyte(c);
	         
	         c = rotateRight(c, 1);
	         c = clampSbyte(c);
	         
	         c ^= 0x49;
	         c = clampSbyte(c);
	         
	         deobstr += "" + Character.toString((char)c);
	      }
	      return deobstr;
	}
	
	
	public String encrypt(String data){
		if(data == null)
			return "";
		return OBSF_VER + Base64.encodeBase64String(_encrypt(data).getBytes());
	}
	
	public String decrypt(String encryptedString){
		if(encryptedString == null)
			return "";
		
		if (encryptedString.indexOf(OBSF_VER) == 0) {
			encryptedString = encryptedString.substring(OBSF_VER.length()); 
	      }
		String decodedString = new String(Base64.decodeBase64(encryptedString));
		return _decrypt(decodedString);
	}
	
	
	public static void main(String[] args) throws UnsupportedEncodingException, CVApplicationException {
		
		CVSecurityChannel900 obj = new CVSecurityChannel900("test");
		String[]  home_id_list = {"02aEBAWEhIbA==",
				"02aEBAWFxQWA==",
				"02aEBAWGhoVA==",
				"02aEBAXGhsaA=="};
		String[]  mac_id_list = { "02TExIXMKVbMKVaEhEwpnCmQ==",
				"02bExIbMKJUMKJSEjCncKZwoE=",
				"02bExIbMKJUFBUSMKJwp3CmQ==",
				"02bExIbMKJUFREUFhMSA==",
				"02bExIbMKJUEBQaMKZwolY"};
	

	 for (int i=0 ; i <home_id_list.length;i++)		
		 System.out.println(obj.decrypt(home_id_list[i]));
    System.out.println("----------------------------------------------");
	 for ( int i=0 ; i < mac_id_list.length;i++)		
		 System.out.println(obj.decrypt( mac_id_list[i]));	
		
	}
	
}


	
 
