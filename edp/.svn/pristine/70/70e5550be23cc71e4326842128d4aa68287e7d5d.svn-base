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
				encrypterFactory = new EncrypterFactory(user, nameNodeService);
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
		//List<String> myargs = Arrays.asList("GLOBAL_UNIFIED", "OPTIMUM_ID", "sonali.rawool", "34RplGqYCSoAsrGdOmKSag", "DECRYPT");
		/*List<String> myargs = Arrays.asList("CHANNEL900", "HOME_ID", "sonali.rawool", "SABLRMHCD", "ENCRYPT");
		TupleFactory t = TupleFactory.getInstance();
		String result = dc.exec(t.newTuple(myargs));
		System.out.println("ENCRYPT : "+ result);*/
		
		List<String> myargs1 = Arrays.asList("CHANNEL900", "MAC_ID", "sonali.rawool", "AB82A17E6D105575CEEDD0580D8A93A9", "DECRYPT", "");
		TupleFactory t1 = TupleFactory.getInstance();
		String result1 = dc.exec(t1.newTuple(myargs1));
		System.out.println("DECRYPT : "+result1);
		//List<String> myargs = Arrays.asList("GLOBAL_UNIFIED", "OPTIMUM_ID", "Administrator", "8c4wEixQHdnZD3r1VisuUw==", "DECRYPT");
		//List<String> myargs= Arrays.asList("ONET_PROD","TTT_ACCT_NUMBER", "Administrator", "EyxY89R5TBKHY5NhUIjIEdAkwAtL1qdtIEE9QwphEjQ=","DECRYPT");
		//List<String> myargs = Arrays.asList("ONET_PROD", "OPTIMUM_ID", "Administrator", "34RplGqYCSoAsrGdOmKSag", "DECRYPT");
		//List<String> myargs = Arrays.asList("ONET_PROD_ESP", "OPTIMUM_ID", "Administrator", "8bD0-rFNR1g", "DECRYPT");
		// List<String> myargs= Arrays.asList("VOW_ESP","EMAIL", "MangeshB", "61068dce48a536fbb34f0fd8d36a7945","DECRYPT");
		// List<String> myargs= Arrays.asList("VOW_ESP","EMAIL", "MangeshB", "61068dce48a58d36a7945","DECRYPT");
		/*TupleFactory t = TupleFactory.getInstance();
		String result = dc.exec(t.newTuple(myargs));
		System.out.println(result);*/

	}

}
