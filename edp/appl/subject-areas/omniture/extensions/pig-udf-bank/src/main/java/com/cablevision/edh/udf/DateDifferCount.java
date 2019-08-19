package com.cablevision.edh.udf;

import java.io.IOException;
import java.util.Date;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DateDifferCount extends EvalFunc<Long> {

		private static final Logger logger = LoggerFactory.getLogger(DateDifferCount.class);
		private DateFormat df = new SimpleDateFormat("yyyy-MM-dd");

		@Override
		public Long exec(Tuple input) throws IOException {
			
			Long difference = null;
			
			if (input == null || input.size() == 0) {
				logger.error("Input is null");
				return null;
			}
			
			if (input.size() < 1 ) {
				logger.error("Some Input value is missing");
				return null;
			}

			try {
				
				Date date1 = (Date) df.parse((String) input.get(0));
				Date date2 = (Date) df.parse((String) input.get(1));
				difference = ((date1.getTime()-date2.getTime())/86400000);
				
			}
			
			catch (Exception e) {
				logger.error("Error occurred while counting days differnce between two dates ");
				logger.error(e.getMessage(), e);
			}
			return difference;

		}

		public static void main(String[] args) throws Exception {
			DateDifferCount dc = new DateDifferCount();
			List<String> myargs1 = Arrays.asList("2016-02-15","2016-03-15");
			TupleFactory t1 = TupleFactory.getInstance();
			Long result1 = dc.exec(t1.newTuple(myargs1));
			System.out.println("Days Difference: "+result1);
		}

	}

