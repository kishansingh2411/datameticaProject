package com.cablevision.edh.udf;

import org.apache.commons.codec.binary.Base64;

public class Channel900EncryptionUtil {
	
	private final String OBSF_VER = "02";
	
	public static void main(String[] args){
		Channel900EncryptionUtil encUtil = new Channel900EncryptionUtil();
		String encryptedString = encUtil.encrypt("asdas");
		System.out.println(encryptedString);
		String originalString = encUtil.decrypt(encryptedString);
		System.out.println(originalString);
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
	

	private String _encrypt(String data) {
		String obstr = "";
		int len = data.length();
		
		for (int i=0; i < len; i++){
			int charectorCode = data.codePointAt(i);
			charectorCode ^= 0x49;
			charectorCode = clampSbyte(charectorCode);
			
			charectorCode = rotateLeft(charectorCode, 1);
			charectorCode = clampSbyte(charectorCode);
			
			charectorCode ^= 0xD4;
			charectorCode = clampSbyte(charectorCode);
	         
			charectorCode = rotateLeft(charectorCode, 1);
			charectorCode = clampSbyte(charectorCode);
			
			obstr += "" + Character.toString((char)charectorCode);
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
	
	  private int rotateRight(int value, int positions){
		      return (((value & 0xFF) >>> positions) | (((value & 0xFF) << (32 - positions)) >>> 24));
	  }
	
	
	private int clampSbyte (int charectorCode){
		if(charectorCode >= 128){
			charectorCode -= 256;
		}
		return charectorCode;
	}
	
	private int rotateLeft (int value, int positions){
		int rotated = (((value & 0xFF) << positions) | (((value & 0xFF) << positions) >> 8));
		return rotated;
	}
}
