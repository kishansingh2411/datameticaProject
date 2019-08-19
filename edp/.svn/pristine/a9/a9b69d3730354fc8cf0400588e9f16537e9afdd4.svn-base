package com.cablevision.edh.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplaceLeadingZero extends EvalFunc<String> {

	private static final Logger logger = LoggerFactory.getLogger(ReplaceLeadingZero.class);

	@Override
	public String exec(Tuple input) throws IOException {
		String result = null;
		if (input == null || input.size() == 0) {
			logger.error("Input is null");
			return null;
		}
		
		if (input.size() < 1 ) {
			logger.error("Some Input value is missing");
			return null;
		}

		try {
			String fieldValue = input.get(0).toString();
			result = fieldValue.replaceAll("\\G0", " ");
		}
		
		catch (Exception e) {
			logger.error("Error occurred while replacing leading zeros");
			logger.error(e.getMessage(), e);
		}

		return result;
	}

	public static void main(String[] args) throws Exception {
		ReplaceLeadingZero dc = new ReplaceLeadingZero();
		List<String> myargs1 = Arrays.asList("000008970004567", "0", " ");
		TupleFactory t1 = TupleFactory.getInstance();
		String result1 = dc.exec(t1.newTuple(myargs1));
		System.out.println("DECRYPT : "+result1);
	}

}
