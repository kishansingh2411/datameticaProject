package com.cablevision.edh.udf;

import java.io.IOException;
import java.util.LinkedList;

import org.apache.pig.AccumulatorEvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AssignHouseNumber extends AccumulatorEvalFunc<DataBag> {
	
	Logger logger = LoggerFactory.getLogger(AssignHouseNumber.class);

	DataBag resultBag = BagFactory.getInstance().newDefaultBag();
	
	@Override
	public void accumulate(Tuple b) throws IOException {
		
		try {
				
				LinkedList<String> corpHouseList = new LinkedList<String>();
				
				String corpHouse = "-1_-1";
				for (Tuple t: (DataBag)b.get(0))
				{
					logger.info("Tuple length "+t.size());
					Tuple t_new = TupleFactory.getInstance().newTuple(t.getAll());
					resultBag.add(t_new);
					DateTime visitDate = (DateTime) t.get(479);
					String dwellingNo = (String) t.get(494);
					Integer corp = (Integer) t.get(493);
					String cartId = (String) t.get(492);
					String postVisidHigh = (String) t.get(353);
					String postVisidLow = (String) t.get(354);
					String ip = (String) t.get(128);
					logger.info("cart id "+cartId+", post visid high "+postVisidHigh+", post visid low "+postVisidLow+", ip "+ip+", visit date "+visitDate+", corp "+corp+", dwelling no "+dwellingNo);
					if (corp != null && corp != -1 && dwellingNo != null && !dwellingNo.equals("-1") && !(corp+"_"+dwellingNo).equals(corpHouse))
					{
						corpHouse = corp+"_"+dwellingNo.trim();
						corpHouseList.add(corpHouse);
					}
					else
					{
						logger.info("casting corp to int");
						t_new.set(493, Integer.parseInt(corpHouse.split("_")[0]));
						t_new.set(494, corpHouse.split("_")[1]);
					}
					
				}
				
				logger.info("corp house list "+corpHouseList);
				
				for (Tuple record: resultBag)
				{
				
					if (((String) record.get(494)) == null || ((String) record.get(494)).equals("-1"))
					{
						logger.info("casting corp to int again");
						record.set(493, Integer.parseInt(corpHouseList.get(0).split("_")[0]));
						record.set(494, corpHouseList.get(0).split("_")[1]);
					}
					
				}
				
			} catch (Exception e) {
				
				logger.error(e.getMessage(), e);
			
			}
	}

	@Override
	public void cleanup() {
		
		this.resultBag = BagFactory.getInstance().newDefaultBag();
	}

	@Override
	public DataBag getValue() {
		
		logger.info("size of the result records "+resultBag.size());
		return resultBag;
	}
	
	@Override
	public Schema outputSchema(Schema input)
	 {
	   try {
	     Schema.FieldSchema inputFieldSchema = input.getField(0);

	     if (inputFieldSchema.type != DataType.BAG)
	     {
	       throw new RuntimeException("Expected a BAG as input");
	     }
	     
	     Schema inputBagSchema = inputFieldSchema.schema;
	     
	     if (inputBagSchema.getField(0).type != DataType.TUPLE)
	     {
	       throw new RuntimeException(String.format("Expected input bag to contain a TUPLE, but instead found %s",
	                                                DataType.findTypeName(inputBagSchema.getField(0).type)));
	     }
	     
	     Schema inputTupleSchema = inputBagSchema.getField(0).schema;
	     
	     Schema outputTupleSchema = inputTupleSchema.clone();   
	     
	     return new Schema(new Schema.FieldSchema(getSchemaName(this.getClass()
	                                                            .getName()
	                                                            .toLowerCase(), input),
	                                          outputTupleSchema,
	                                          DataType.BAG));
	   }
	   catch (CloneNotSupportedException e) {
	     throw new RuntimeException(e);
	   }
	   catch (FrontendException e) {
	     throw new RuntimeException(e);
	   }
	 }

	
}
