package com.cablevision.edh.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class Channel900Decryptor extends EvalFunc<String>{

	private static final Logger logger = LoggerFactory.getLogger(Channel900Decryptor.class);
	Channel900EncryptionUtil encryptorUtil = new Channel900EncryptionUtil();
	String decryptedString = null;
	
	@Override
	public String exec(Tuple input) throws IOException {
		
		logger.info("Channel900 - input -> "+input);
		if (input.get(0) == null)
		{
			logger.info("Null String input passed to Channel 900 Decryptor");
			return null;
		}
		
		String inputString = (String) input.get(0);
		logger.info("Channel900 - String to be Decrypted - " + inputString);
		
		try{
			decryptedString = encryptorUtil.decrypt(inputString);
		}catch(Exception ex){
			logger.info("Channel900 - " + ex.getMessage());
		}
		logger.info("Channel900 - Decrypted String - " + decryptedString);
		return decryptedString;
	}
}
