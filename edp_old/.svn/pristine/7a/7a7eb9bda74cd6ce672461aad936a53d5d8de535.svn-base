package com.cablevision.edh.udf;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ResolvePluginId extends EvalFunc<String>{

	private static final Logger logger = LoggerFactory.getLogger(ResolvePluginId.class);
	
	private final Map<String, String> pluginMap = new HashMap<String, String>();
	
	private final StringBuilder strBuilder = new StringBuilder();

	public ResolvePluginId() {
	}

	@Override
	public String exec(Tuple input) throws IOException {
		logger.info("input -> "+input);
		if (input.get(0) == null)
		{
			logger.info("no plugin ids to resolve");
			return null;
		}
		
		String pluginIds = (String) input.get(0);
		logger.info("plugin ids to be resolved "+pluginIds);
		if (pluginMap.isEmpty())
		{
			DataBag bag = (DataBag)input.get(1);
			logger.info("bag "+bag);
			Iterator<Tuple> iter = bag.iterator();
			iter.next();
			Tuple pluginValues = (Tuple)iter.next();
			DataBag pluginBag = (DataBag)pluginValues.get(0);
			Iterator<Tuple> bagIter = pluginBag.iterator();
			while (bagIter.hasNext())
			{
				Tuple plugin = bagIter.next();
				Tuple pluginTuple = (Tuple)plugin.get(0);
				pluginMap.put((String)pluginTuple.get(0), (String)pluginTuple.get(1));
			}
			
			logger.info("plugin map "+pluginMap);
		}
		
		List<String> pluginIdList = Arrays.asList(pluginIds.split(","));
		
		for(String pluginId: pluginIdList)
		{
			strBuilder.append(pluginMap.get(pluginId.trim()));
			strBuilder.append('|');
		}
		
		
		String result = strBuilder.substring(0, strBuilder.length() - 1);
		logger.info("plugin values "+result);
		strBuilder.delete(0, strBuilder.length());
		return result;
	}

}
