package com.cablevision.shoppingcart;

import java.io.File;
import java.io.FileReader;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cablevision.amss.client.AMSSClient;
import com.cablevision.amss.client.impl.AMSSClientImpl;
import com.cablevision.shoppingcart.dao.ShoppingCartDAO;

public class ShoppingCartUtil {
	
	public static void main(String[] args) throws Exception
	{
		System.out.println("arguments passed "+Arrays.asList(args));
		String logDir = ".";
		if (args.length == 2)
		{
			if (args[1].endsWith("/"))
			{
				logDir = StringUtils.removeEnd(args[1], "/");
			}
			else
			{
				logDir = args[1];
			}
		}
				
		System.setProperty("log.dir", logDir);
		Logger logger = LoggerFactory.getLogger(ShoppingCartUtil.class);
		logger.info("arguments passed "+Arrays.asList(args));

		Properties props = new Properties();
		props.load(new FileReader(new File(args[0])));
		
		BigDecimal key_param_id = new BigDecimal(Integer.parseInt(StringUtils.trim(props.getProperty("key_param_id"))));
		logger.info("key param id "+key_param_id);
		int batch_size = Integer.parseInt(StringUtils.trim(props.getProperty("batch_size", "25")));
		logger.info("batch size "+batch_size);
		
		ShoppingCartDAO dao = new ShoppingCartDAO(props);
		
		ParamDTMInfo currentParamDTMInfo = dao.getKeyParamDTMInfo(key_param_id);
		Map<BigDecimal, String> cartIdMap = dao.getModifiedShoppingCartIds(key_param_id);

		TreeMap<BigDecimal, String> sortedIdMap = new TreeMap<BigDecimal, String>();
		sortedIdMap.putAll(cartIdMap);
		
		Map<String, BigDecimal> cartIdChangeSeqIdMap = new HashMap<String, BigDecimal>();
		
		for (Map.Entry<BigDecimal, String> entry: sortedIdMap.entrySet())
		{
			cartIdChangeSeqIdMap.put(entry.getValue(), entry.getKey());
		}
		
		
		java.sql.Timestamp updatedParamDTMStartTime = new java.sql.Timestamp(currentParamDTMInfo.getDtmStartDate().getTime() + currentParamDTMInfo.getInterval()*1000);
		java.sql.Timestamp updatedParamDTMEndTime = new java.sql.Timestamp(updatedParamDTMStartTime.getTime() + currentParamDTMInfo.getInterval()*1000);

		LogInfo logInfo = new LogInfo();
		
		Set<String> uniqueCartIdSet = new HashSet<String>();
		uniqueCartIdSet.addAll(sortedIdMap.values());
		
		if (cartIdMap.isEmpty())
		{
			logger.warn("No modified shopping carts found");
			System.out.println("No modified shopping carts found");
			return;
		}
		
		logger.info("Modified carts "+sortedIdMap);
		AMSSClient client = new AMSSClientImpl(props);
		String userId = StringUtils.trim(props.getProperty("amss.service.userid"));
		String password = StringUtils.trim(props.getProperty("amss.service.password"));
		logger.info("authenticating to obtain the token");
		String token = client.login(userId, password);
		logger.info("obtained the security token");
		Set<String> cartIdSet = new LinkedHashSet<String>();
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		String currentDateStr = sdf.format(new Date());
		
		logInfo.setKeyParamId(key_param_id);
		logInfo.setSequenceId(currentDateStr);
		logInfo.setDtmStartTime(new java.sql.Timestamp(System.currentTimeMillis()));
		logInfo.setH_shoppingCartIdBegin(sortedIdMap.firstEntry().getKey());
		logInfo.setH_shoppingCartIdEnd(sortedIdMap.lastEntry().getKey());
		
		
		if (sortedIdMap.size() <= batch_size)
		{
			for (Map.Entry<BigDecimal, String> entry: sortedIdMap.entrySet())
			{
				cartIdSet.add(entry.getValue());
			}
			
			ShoppingCartResult result = client.retrieveCartProductListInfo(token, userId, password, cartIdSet, "en_US", "WEB");
			logInfo.setDtmEndTime(new java.sql.Timestamp(System.currentTimeMillis()));
			logInfo.setCartIdTotalRequesCount(result.getTotalNoOfRequests());
			logInfo.setCartIdNoResponseCount(result.getNoOfUnsucessfulResponses());
			logInfo.setCartIdSuccessRequestCount(result.getNoOfSuccessfulResponses());
			logInfo.setDtmCreatedTime(new java.sql.Timestamp(System.currentTimeMillis()));
			BigDecimal lastChangeSequenceId = sortedIdMap.lastKey();
			logger.info("Persisting shopping cart information into database");
			dao.saveShoppingCartInfo(result.getCartProductListInfo(), lastChangeSequenceId, currentDateStr, updatedParamDTMStartTime, updatedParamDTMEndTime);
			dao.saveStatisticsInfo(logInfo, result.getMissingProductOfferingInfo(), currentDateStr, cartIdChangeSeqIdMap);
		}
		else 
		{
			int counter = 0;
			int noOfTotalRequests = 0;
			int noOfSuccessfulRequests = 0;
			int noOfUnsuccessfulRequests = 0;
			Map<String, MissingProductOffering> missingProductOfferMap = new HashMap<String, MissingProductOffering>();
			
			for (Map.Entry<BigDecimal, String> entry: sortedIdMap.entrySet())
			{
				counter++;
				if (uniqueCartIdSet.contains(entry.getValue()))
					cartIdSet.add(entry.getValue());
				
				if(counter == sortedIdMap.size() || counter % batch_size == 0)
				{
					BigDecimal cartChangeSequenceId = entry.getKey();
					ShoppingCartResult result = client.retrieveCartProductListInfo(token, userId, password, cartIdSet, "en_US", "WEB");
					noOfTotalRequests += result.getTotalNoOfRequests();
					noOfSuccessfulRequests += result.getNoOfSuccessfulResponses();
					noOfUnsuccessfulRequests += result.getNoOfUnsucessfulResponses();
					missingProductOfferMap.putAll(result.getMissingProductOfferingInfo());
					logger.info("Persisting shopping cart information into database");
					dao.saveShoppingCartInfo(result.getCartProductListInfo(), cartChangeSequenceId, currentDateStr, updatedParamDTMStartTime, updatedParamDTMEndTime);
					uniqueCartIdSet.removeAll(cartIdSet);
					logger.info("cartIdSet before "+cartIdSet);
					cartIdSet.removeAll(cartIdSet);
					logger.info("cartIdSet after "+cartIdSet);
				}					
				
			}
			
			logInfo.setDtmEndTime(new java.sql.Timestamp(System.currentTimeMillis()));
			logInfo.setCartIdTotalRequesCount(noOfTotalRequests);
			logInfo.setCartIdNoResponseCount(noOfUnsuccessfulRequests);
			logInfo.setCartIdSuccessRequestCount(noOfSuccessfulRequests);
			logInfo.setDtmCreatedTime(new java.sql.Timestamp(System.currentTimeMillis()));
			dao.saveStatisticsInfo(logInfo, missingProductOfferMap, currentDateStr, cartIdChangeSeqIdMap);
		}

	}
}
