package com.cablevision.edh.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EncryptDecryptUtil extends EvalFunc<String> {

	private static final Logger logger = LoggerFactory.getLogger(EncryptDecryptUtil.class);

	EncrypterFactory encrypterFactory;

	@Override
	public String exec(Tuple input) throws IOException {

		String result = null;
		if (input == null || input.size() == 0) {
			logger.error("input is null");
			return null;
		}
		
		if (input.get(3) == null ) {
			logger.error("Input value is missing");
			return null;
		}

		try {
			String suiteName = input.get(0).toString();
			String fieldName = input.get(1).toString();
			String user = input.get(2).toString();
			String value = input.get(3).toString();
			String mode = input.get(4).toString();
			String nameNodeService = input.get(5).toString();

			logger.info("suitename " + suiteName + ", fieldName " + fieldName + ", user " + user + ", mode " + mode + ", nameNodeService " + nameNodeService);

			if (encrypterFactory == null) {
				encrypterFactory = new EncrypterFactory(user,nameNodeService);
			}

			EncryptDecryptFunc encryptDecryptFunc = encrypterFactory.getEncryptDecryptFunc(suiteName, fieldName);

			if ("Decrypt".equalsIgnoreCase(mode)) {
				result = encryptDecryptFunc.decrypt(fieldName, value);

				result = result.isEmpty() ? null : result;
			} else {
				result = encryptDecryptFunc.encrypt(fieldName, value);
				result = result.isEmpty() ? null : result;
			}

		}
		catch (EncrypterInstantiationException e)
		{
			logger.error("Error occurred while trying to initialize");
			throw new IOException(e);
		}
		catch (Exception e) {
			logger.error("Error occurred while trying to decrypt the record with fields " + input);
			logger.error(e.getMessage(), e);
		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		EncryptDecryptUtil dc = new EncryptDecryptUtil();
		List<String> myargs = Arrays.asList("MRDVR", "HOME_ID", "sagar.roman", "3506888", "ENCRYPT");
		TupleFactory t = TupleFactory.getInstance();
		String result = dc.exec(t.newTuple(myargs));
		System.out.println("ENCRYPT : "+ result);
		
		List<String> myargs1 = Arrays.asList("MRDVR", "HOME_ID", "sonali.rawool", result, "DECRYPT");
		TupleFactory t1 = TupleFactory.getInstance();
		String result1 = dc.exec(t1.newTuple(myargs1));
		System.out.println("DECRYPT : "+result1);
		/*TupleFactory t = TupleFactory.getInstance();
		String result = dc.exec(t.newTuple(myargs));
		System.out.println(result);*/

	}

}
