package com.cablevision.edh.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Channel900Encryptor extends EvalFunc<String>{
	
	private static final Logger logger = LoggerFactory.getLogger(Channel900Encryptor.class);
	Channel900EncryptionUtil encryptorUtil = new Channel900EncryptionUtil();
	String encryptedString = null;
	
	@Override
	public String exec(Tuple input) throws IOException {
		
		logger.info("Channel900 - input -> "+input);
		if (input.get(0) == null)
		{
			logger.info("Null String input passed to Channel 900 Encryptor");
			return null;
		}
		
		String inputString = (String) input.get(0);
		logger.info("Channel900 - String to be Encrypted - " + inputString);
		
		try{
			encryptedString = encryptorUtil.encrypt(inputString);
		}catch(Exception ex){
			logger.info("Channel900 - " + ex.getMessage());
		}
		logger.info("Channel900 - Enctypted String - " + encryptedString);
		return encryptedString;
	}
}
