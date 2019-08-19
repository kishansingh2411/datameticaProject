package com.cablevision.amss.client.impl;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.xml.bind.JAXBException;
import javax.xml.namespace.QName;
import javax.xml.ws.soap.SOAPBinding;
import javax.xml.ws.soap.SOAPFaultException;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.jxpath.JXPathContext;
import org.apache.commons.jxpath.Pointer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.cxf.endpoint.Client;
import org.apache.cxf.frontend.ClientProxy;
import org.apache.cxf.interceptor.LoggingInInterceptor;
import org.apache.cxf.interceptor.LoggingOutInterceptor;
import org.apache.cxf.ws.security.wss4j.WSS4JOutInterceptor;
import org.apache.wss4j.dom.WSConstants;
import org.apache.wss4j.dom.handler.WSHandlerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amdocs.cih.services.css.common.datatypes.LoginInfo;
import com.amdocs.cih.services.css.interfaces.ejb.ws.SecurityTokenServiceSvc;
import com.amdocs.cih.services.css.interfaces.ejb.ws.SecurityTokenServiceSvcService;
import com.amdocs.informationmodel.productdomain.product.ProductOfferingInstance;
import com.amdocs.informationmodel.productdomain.productoffering.ProductOffering;
import com.amdocs.salesengine.impl.services.ImplRateCodeInfo;
import com.amdocs.services.common.SalesContext;
import com.amdocs.services.impl.order.ObjectFactory;
import com.amdocs.services.impl.order.RetrieveShoppingCartInput;
import com.amdocs.services.impl.order.RetrieveShoppingCartOutput;
import com.amdocs.services.impl.order.ejb.ws.ImplWSOrder;
import com.amdocs.services.impl.order.ejb.ws.ImplWSOrderService;
import com.cablevision.amss.client.AMSSClient;
import com.cablevision.amss.client.AMSSServiceException;
import com.cablevision.shoppingcart.CartProductListInfo;
import com.cablevision.shoppingcart.MissingProductOffering;
import com.cablevision.shoppingcart.ProductOfferingInfo;
import com.cablevision.shoppingcart.RateCodeInfo;
import com.cablevision.shoppingcart.ShoppingCartResult;

/**
 *
 *
 */
public class AMSSClientImpl implements AMSSClient
{
	private static final Logger logger = LoggerFactory.getLogger(AMSSClientImpl.class);
	private ImplWSOrderService implOrderService;
	private ImplWSOrder orderService;
	private SecurityTokenServiceSvcService secService;
	private final ObjectFactory objFactory = new ObjectFactory();
	com.amdocs.services.common.ObjectFactory commonObjFactory = new com.amdocs.services.common.ObjectFactory();
	private int maxErrorThreshold = 5;
	private boolean enableResponseLogging = false;
	private int noOfRetryAttempts = 3;
	private int intervalBetweenRetries = 15;
	private List<String> shoppingCartRateCodeAttributes = new ArrayList<String>();
	private List<String> shoppingCartProductOfferingAttributes = new ArrayList<String>();
	
	public AMSSClientImpl (Properties props) throws MalformedURLException
	{
		String orderServiceURL = StringUtils.trim(props.getProperty("amss.order.service.url"));
		String securityServiceURL = StringUtils.trim(props.getProperty("amss.security.service.url"));
		maxErrorThreshold = Integer.parseInt(props.getProperty("max_service_error_threshold", "5"));
		enableResponseLogging = Boolean.parseBoolean(props.getProperty("enable_response_logging", "false"));
		noOfRetryAttempts = Integer.parseInt(props.getProperty("no_of_retry_attempts", "3"));
		intervalBetweenRetries = Integer.parseInt(props.getProperty("interval_between_retries", "15"));
		logger.info("web service urls provided "+orderServiceURL+","+securityServiceURL);
		logger.info("max error threshold "+maxErrorThreshold);
		URL wsdlURL = getClass().getResource("ImplWSOrder.wsdl");
		QName qName = new QName("http://ws.ejb.order.impl.services.amdocs.com", "ImplWSOrder");	
		URL secwsdlURL = getClass().getResource("SecurityTokenServiceSvc.wsdl");
		QName secQName = new QName("http://ws.ejb.interfaces.css.services.cih.amdocs.com", "SecurityTokenServiceSvc");
		implOrderService = new ImplWSOrderService(wsdlURL, qName);
		secService = new SecurityTokenServiceSvcService(secwsdlURL, secQName);
		implOrderService.addPort(qName, SOAPBinding.SOAP11HTTP_BINDING, orderServiceURL);
		secService.addPort(secQName, SOAPBinding.SOAP11HTTP_BINDING, securityServiceURL);
		String mandatoryRateCodeAttrs = StringUtils.trim(props.getProperty("mandatory.ratecode.attributes"));
		String mandatoryProductOfferingAttrs = StringUtils.trim(props.getProperty("mandatory.productoffering.attributes"));
		
		initializeMandatoryShoppingCartAttributesList(mandatoryRateCodeAttrs, mandatoryProductOfferingAttrs);
	}
	
	private void initializeMandatoryShoppingCartAttributesList(String mandatoryRateCodeAttrs, String mandatoryProductOfferingAttrs) {
		
		for (String attr: Arrays.asList(mandatoryRateCodeAttrs.split(",")))
		{
			shoppingCartRateCodeAttributes.add(attr.trim());
		}
		
		for (String attr: Arrays.asList(mandatoryProductOfferingAttrs.split(",")))
		{
			shoppingCartProductOfferingAttributes.add(attr.trim());
		}
		
	}

	@Override
	public ShoppingCartResult retrieveCartProductListInfo(String token, String userId, String password, Set<String> cartIdSet, String locale, String salesChannel)
		throws AMSSServiceException
	{
		logger.info("Retreiving shopping cart information for the following cart ids "+cartIdSet);
    	orderService = implOrderService.getImplWSOrder();
    	
    	Client client = ClientProxy.getClient(orderService);
    	org.apache.cxf.endpoint.Endpoint cxfEndpoint = client.getEndpoint();
    	Map<String,Object> outProps = new HashMap<String,Object>();
    	// how to configure the properties is outlined below;
    	outProps.put(WSHandlerConstants.ACTION, WSHandlerConstants.USERNAME_TOKEN);
    	
    	// Password type : plain text
    	outProps.put(WSHandlerConstants.PASSWORD_TYPE, WSConstants.PW_TEXT);
    	
//    	outProps.put(WSHandlerConstants.USER, "Tksmau64vLAoMRtjAbbvl6336fSOMj3tlFoa}Zj0");
    	outProps.put(WSHandlerConstants.USER, token);

    	// Callback used to retrieve password for given user.
    	outProps.put(WSHandlerConstants.PW_CALLBACK_CLASS, 
    	    ClientPasswordCallback.class.getName());
    	
    	WSS4JOutInterceptor wssOut = new WSS4JOutInterceptor(outProps);
    	cxfEndpoint.getOutInterceptors().add(wssOut);
    	LoggingOutInterceptor logOutInterceptor = new LoggingOutInterceptor();
    	LoggingInInterceptor logInInterceptor = new LoggingInInterceptor();
    	logOutInterceptor.setPrettyLogging(true);
    	logInInterceptor.setPrettyLogging(true);
    	logInInterceptor.setLimit(-1);
    	cxfEndpoint.getOutInterceptors().add(logOutInterceptor);
    	
    	if(enableResponseLogging)
    		cxfEndpoint.getInInterceptors().add(logInInterceptor);
    	
    	
    	RetrieveShoppingCartInput sc= objFactory.createRetrieveShoppingCartInput();
    	
    	List<CartProductListInfo> cartProductInfoList = new ArrayList<CartProductListInfo>();
    	
    	logger.info("retreiving shopping cart information for cart ids "+cartIdSet);
    	
    	
    	
    	
    	int noOfErrors = 0;
    	int noOfRequests = 0;
    	int noOfSuccessResponses = 0;
    	int noOfUnSucessfulResponses = 0;
    	
    	Map<String, MissingProductOffering> missingProductOfferingMap = new HashMap<String, MissingProductOffering>();
    	
    	for(String cartId: cartIdSet)
    	{
    		logger.info("retreiving cart information for cart id "+cartId);
    		sc.setCartId(objFactory.createRetrieveShoppingCartInputCartId(StringUtils.trim(cartId)));
        	SalesContext salesCtx = new SalesContext();
        	salesCtx.setLocaleString(commonObjFactory.createApplicationContextLocaleString(locale));
        	salesCtx.setSalesChannel(commonObjFactory.createSalesContextSalesChannel(salesChannel));
        	sc.setSaleContext(objFactory.createRetrieveShoppingCartInputSaleContext(salesCtx));
        	sc.setExcludeProductConfiguration(objFactory.createRetrieveShoppingCartInputExcludeProductConfiguration(false));
        	RetrieveShoppingCartOutput shoppingCartoutput = null;

			noOfRequests++;
			boolean attemptRequest = true;
			int retryAttempt = 0;
			boolean continueOnError = false;
			
			while (attemptRequest && retryAttempt <= noOfRetryAttempts)
			{
				try {
					
					shoppingCartoutput = orderService.retrieveShoppingCart(sc);
					
					attemptRequest = false;
				} 
				catch (Exception e) 
				{
					logger.error("Following exception encountered while trying to retrieve shopping cart information for cart id "+cartId);
					logger.error(e.getMessage(),e);
					
					if (e instanceof SOAPFaultException && ((SOAPFaultException)e).getFault().getFaultString().contains("AuthenticationException"))
					{
						logger.info("Authentication exception encountered, attempting login again");
						
						try {
							token = login(userId, password);
							outProps.put(WSHandlerConstants.USER, token);
							shoppingCartoutput = orderService.retrieveShoppingCart(sc);
							attemptRequest = false;
						} catch (SOAPFaultException e1) {
							// TODO Auto-generated catch block
							logger.error("Following error occured during retry login attempt");
							logger.error(e1.getMessage(), e1);
							noOfErrors++;
							noOfUnSucessfulResponses++;
							if (noOfErrors <= maxErrorThreshold)
							{
								logger.warn("Total no of errors occurred "+noOfErrors);
								continueOnError = true;
							}
							else
							{
								logger.error("Total no of errors crossed the error threshold value, exiting.");
								throw new AMSSServiceException("Total no of errors crossed the error threshold value, exiting.");
							}
							
						}
					}
					else
					{
						retryAttempt++;
						if (retryAttempt <= noOfRetryAttempts)
						{
							logger.info("Calling retrieve shopping cart service with retry attempt "+retryAttempt);
							try {
								Thread.sleep(intervalBetweenRetries);
							} catch (InterruptedException e1) {
								throw new AMSSServiceException(e.getMessage());
							}
						}
						else
						{
							logger.error("Calling retrieve shopping cart service for cart id "+cartId+" failed after "+retryAttempt+" retry attempts");
							noOfErrors++;
							noOfUnSucessfulResponses++;
							if (noOfErrors <= maxErrorThreshold)
							{
								logger.warn("Total no of errors occurred "+noOfErrors);
								continueOnError = true;
							}
							else
							{
								logger.error("Total no of errors crossed the error threshold value, exiting.");
								throw new AMSSServiceException("Total no of errors crossed the error threshold value, exiting.");
							}
						}
					}
				}
				
				
			}
			
			if (continueOnError)
			{
				continue;
			}
		
			if (shoppingCartoutput.getResponseInfo() != null && shoppingCartoutput.getResponseInfo().getValue() != null &&
					shoppingCartoutput.getResponseInfo().getValue().getStatusCode() != null)
			{
	        	if(shoppingCartoutput.getResponseInfo().getValue().getStatusCode().getValue().equals("0000000000"))
	        	{
	        		logger.info("retreived cart information for cart id "+cartId);
	        		noOfSuccessResponses++;
	        	}
	        	else if(shoppingCartoutput.getResponseInfo().getValue().getStatusCode().getValue().equals("1001000008"))
	        	{
	        		logger.warn("cart id "+cartId+" does not exist");
	        		noOfUnSucessfulResponses++;
	        		continue;
	        	}
	        	else if(shoppingCartoutput.getResponseInfo().getValue().getStatusCode().getValue().equals("1001000001"))
	        	{
	        		logger.warn("cart id "+cartId+" is not valid");
	        		noOfUnSucessfulResponses++;
	        		continue;
	        	}
	        	else if(shoppingCartoutput.getResponseInfo().getValue().getStatusCode().getValue().equals("1001000005"))
	        	{
	        		logger.warn("sales context with sales channel "+salesChannel+" is not valid");
	        		noOfUnSucessfulResponses++;
	        		continue;
	        	}
	        	else
	        	{
	        		logger.warn("could not retrieve cart information for cart id "+cartId);
	        		noOfUnSucessfulResponses++;
	        		continue;
	        	}
			}
			else
			{
				logger.warn("MISSING CART ATTRIBUTE: response info is missing for shopping cart "+cartId);
			}
        
        	CartProductListInfo cartProductListInfo = new CartProductListInfo();
        	
        	if (shoppingCartoutput.getCartId() != null && StringUtils.isNotBlank(shoppingCartoutput.getCartId().getValue()))
        	{
        		cartProductListInfo.setCartId(shoppingCartoutput.getCartId().getValue());
        	}
        	else
        	{
        		logger.warn("MISSING CART ATTRIBUTE: Cart id is missing for the shopping cart "+shoppingCartoutput);
        		logger.warn("MISSING CART ATTRIBUTE: Continuing with the next shopping cart");
        		continue;
        	}

			JXPathContext ctx = JXPathContext.newContext(shoppingCartoutput);
			ctx.setLenient(true);
			logger.info("cart id "+ctx.getValue("cartId"));
			
			boolean missingAttributesFlag = false;
			boolean invalidAttributesFlag = false;
			
        	if(CollectionUtils.isNotEmpty(shoppingCartoutput.getRateCodeList()))
        	{
        	
	        	List<RateCodeInfo> rateCodeInfoList = new ArrayList<RateCodeInfo>();
	        	
	        	int i= 0;
	        	for(ImplRateCodeInfo rateCode: shoppingCartoutput.getRateCodeList())
	        	{
	        		i++;
	        		Pointer rateCodePointer = ctx.getPointer("rateCodeList["+i+"]");
	        		JXPathContext relativeCtx = ctx.getRelativeContext(rateCodePointer);
	        		
	        		if (checkIfAttributesPresent(cartId, relativeCtx, shoppingCartRateCodeAttributes, rateCodePointer.asPath()))
	        		{
		        		RateCodeInfo rateCodeInfo = new RateCodeInfo();
		        		
		        		if (rateCode.getChargeTypeX9() != null)
		        			rateCodeInfo.setChargeTypeX9(rateCode.getChargeTypeX9().getValue());
		        		
		        		if (rateCode.getDurationX9() != null )
		        		{
			        		if (NumberUtils.isNumber(rateCode.getDurationX9().getValue()))
			        		{
			        			rateCodeInfo.setDurationX9(NumberUtils.toInt(rateCode.getDurationX9().getValue()));
			        		}
			        		else
			        		{
			        			invalidAttributesFlag = true;
			        			logger.warn("MISSING CART ATTRIBUTE: rate code duration attribute "+rateCode.getDurationX9().getValue()+" should be a number");
			        		}
		        		}
		        		
		        		if (rateCode.getPartTypeX9() != null)
		        			rateCodeInfo.setPartTypeX9(rateCode.getPartTypeX9().getValue());
		        		
		        		if (rateCode.getPremiumServiceFlagX9() != null)
		        			rateCodeInfo.setPremiumServiceFlagX9(rateCode.getPremiumServiceFlagX9().getValue());
		        		
		        		
		        		if (rateCode.getPriceX9() != null)
		        		{
		        			if (NumberUtils.isNumber(rateCode.getPriceX9().getValue()))
			        		{
			        			rateCodeInfo.setPriceX9(NumberUtils.toFloat(rateCode.getPriceX9().getValue()));
			        		}
			        		else
			        		{
			        			invalidAttributesFlag = true;
			        			logger.warn("MISSING CART ATTRIBUTE: rate code price attribute "+rateCode.getPriceX9().getValue()+" should be a number");

			        		}
		        		}
		        		
		        		
		        		if (rateCode.getProductTypeX9() != null)
		        			rateCodeInfo.setProductTypeX9(rateCode.getProductTypeX9().getValue());
		        		
		        		if (rateCode.getPromoPriceX9() != null)
		        		{
		        			if (NumberUtils.isNumber(rateCode.getPromoPriceX9().getValue()))
			        		{
			        			rateCodeInfo.setPromoPriceX9(NumberUtils.toFloat(rateCode.getPromoPriceX9().getValue()));
			        		}
			        		else
			        		{
			        			invalidAttributesFlag = true;
			        			logger.warn("MISSING CART ATTRIBUTE: rate code promo price attribute "+rateCode.getPromoPriceX9().getValue()+" should be a number");
			        		}
		        		}
		        		
		        		if (rateCode.getPromotionIndX9() != null)
		        			rateCodeInfo.setPromotionIndX9(rateCode.getPromotionIndX9().getValue());
		        		
		        		if (rateCode.getRateCodeX9() != null)
		        			rateCodeInfo.setRateCodeX9(rateCode.getRateCodeX9().getValue());
		        		
		        		if (rateCode.getStandardRateX9() != null)
		        			rateCodeInfo.setStandardRateX9(rateCode.getStandardRateX9().getValue());
		        		
		        		if (rateCode.getWinDurationX9() != null)
		        		{
		        			if (NumberUtils.isNumber(rateCode.getWinDurationX9().getValue()))
			        		{
		        				rateCodeInfo.setWinDurationX9(NumberUtils.toInt(rateCode.getWinDurationX9().getValue()));
			        		}
			        		else
			        		{
			        			invalidAttributesFlag = true;
			        			logger.warn("MISSING CART ATTRIBUTE: rate code win duration attribute "+rateCode.getPromoPriceX9().getValue()+" should be a number");
			        		}
		        		}
		        		
		        		rateCodeInfoList.add(rateCodeInfo);
	        		}
	        		else
	        		{
	        			missingAttributesFlag = true;
	        		}
	        	}
	        	
	        	if (!missingAttributesFlag && !invalidAttributesFlag)
	        	{
	        		cartProductListInfo.setRateCodeList(rateCodeInfoList);
	        	}
	        	
        	}
        	else
        	{
        		MissingProductOffering missingProductOffering = (missingProductOfferingMap.get(cartId) == null) ? new MissingProductOffering() : missingProductOfferingMap.get(cartId);
        		missingProductOffering.setProductNull(true);
        		missingProductOfferingMap.put(cartId, missingProductOffering);
        	}
        	
        	if(CollectionUtils.isNotEmpty(shoppingCartoutput.getProductOfferingsConfiguration()))
        	{
	        	List<ProductOfferingInstance> productOfferingList = shoppingCartoutput.getProductOfferingsConfiguration();
	        	
	        	List<ProductOfferingInfo> productOfferingInfoList = new ArrayList<ProductOfferingInfo>();
	        	
	        	Map<String, ProductOfferingInfo> offerCountMap = new HashMap<String, ProductOfferingInfo>();
	        	
	        	int j = 0;
	        	for (ProductOfferingInstance productOfferingInstance: productOfferingList)
	        	{
	        		j++;
	        		Pointer productOfferingPointer = ctx.getPointer("productOfferingsConfiguration["+j+"]");
	        		JXPathContext relativeCtx = ctx.getRelativeContext(productOfferingPointer);

	        		if (productOfferingInstance.getProductOffering() != null && productOfferingInstance.getProductOffering().getValue().getID() != null &&
	        				 checkIfAttributesPresent(cartId, relativeCtx, shoppingCartProductOfferingAttributes, productOfferingPointer.asPath()))
	        		{
        				ProductOffering productOffer = productOfferingInstance.getProductOffering().getValue();
        				String offerId = productOffer.getID().getValue();
        				if (offerCountMap.containsKey(offerId))
		        		{
		        			ProductOfferingInfo productOfferingInfo = offerCountMap.get(offerId);
		        			productOfferingInfo.setOfferCount(productOfferingInfo.getOfferCount() + 1);
		        		}
		        		else
		        		{
		        			ProductOfferingInfo productOfferingInfo = new ProductOfferingInfo();
		        			if (CollectionUtils.isNotEmpty(productOffer.getNames()))
		        			{
		        				if (productOffer.getNames().get(0).getText() != null)
		        				{
		        					String offerName = productOffer.getNames().get(0).getText().getValue();
		        					productOfferingInfo.setOfferName(offerName);
		        				}
		        			}
		        			
		        			if (CollectionUtils.isNotEmpty(productOffer.getDescriptions()))
		        			{
		        				if (productOffer.getDescriptions().get(0).getText() != null)
		        				{
		        					String offerDescription = productOffer.getDescriptions().get(0).getText().getValue();
		        					productOfferingInfo.setOfferDescription(offerDescription);
		        				}
		        				
		        			}
		            		
		            		productOfferingInfo.setOfferId(offerId);
		            		productOfferingInfo.setOfferCount(1);
		        			offerCountMap.put(offerId, productOfferingInfo);
		        			productOfferingInfoList.add(productOfferingInfo);
		        		}
	        		}
	        		else
	        		{
	        			missingAttributesFlag = true;
	        		}
	        		        		
	        	}
	
	        	if (!missingAttributesFlag)
	        	{
	        		cartProductListInfo.setProductOfferingList(productOfferingInfoList);
	        	}
	        	
        	}
        	else
        	{
        		MissingProductOffering missingProductOffering = (missingProductOfferingMap.get(cartId) == null) ? new MissingProductOffering() : missingProductOfferingMap.get(cartId);
        		missingProductOffering.setOfferNull(true);
        		missingProductOfferingMap.put(cartId, missingProductOffering);
        	}
        	
        	if (!missingAttributesFlag && !invalidAttributesFlag)
        	{
        		cartProductInfoList.add(cartProductListInfo);
        	}
        	else
        	{
        		logger.warn("MISSING CART ATTRIBUTE: one or more attributes missing or invalid for shopping cart "+cartId);
        		logger.warn("skipping shopping cart "+cartId);
        	}
    	}
    	
    	ShoppingCartResult result = new ShoppingCartResult();
    	result.setCartProductListInfo(cartProductInfoList);
    	result.setMissingProductOfferingInfo(missingProductOfferingMap);
    	result.setTotalNoOfRequests(noOfRequests);
    	result.setNoOfSuccessfulResponses(noOfSuccessResponses);
    	result.setNoOfUnsucessfulResponses(noOfUnSucessfulResponses);
    	return result;
	}
	
	private boolean checkIfAttributesPresent(String cartId, JXPathContext context, List<String> shoppingCartAttributes, String ctxPath) {
		
		boolean result = true;
		
		for (String attrPath: shoppingCartAttributes)
		{
			if (context.getValue(attrPath) == null)
			{
				logger.warn("MISSING CART ATTRIBUTE: attribute "+ctxPath+"/"+attrPath+" missing for cart "+cartId);
				result = false;
			}
		}
		
		return result;
		
	}

	@Override
	public String login(String userId, String password) throws AMSSServiceException
	{
    	SecurityTokenServiceSvc svc = secService.getSecurityTokenServiceSvc();
    	Client client = ClientProxy.getClient(svc);
    	org.apache.cxf.endpoint.Endpoint cxfEndpoint = client.getEndpoint();
    	LoggingOutInterceptor logOutInterceptor = new LoggingOutInterceptor();
    	LoggingInInterceptor logInInterceptor = new LoggingInInterceptor();
    	logOutInterceptor.setPrettyLogging(true);
    	logInInterceptor.setPrettyLogging(true);
    	cxfEndpoint.getOutInterceptors().add(logOutInterceptor);
    	cxfEndpoint.getInInterceptors().add(logInInterceptor);
    	int retryAttempt = 0;
    	boolean attemptFlag = true;
		logger.info("calling the login service");
		String token = "";
		while (attemptFlag && retryAttempt <= noOfRetryAttempts )
		{
			try
			{
		    	LoginInfo loginRetType = svc.login(null, userId, password);
		    	logger.info("response context " +client.getResponseContext());
		    	
		    	if (((Integer) client.getResponseContext().get("org.apache.cxf.message.Message.RESPONSE_CODE")) != 200)
		    	{
		    		retryAttempt++;
		    		logger.warn("Calling login service with retry attempt "+retryAttempt);	
					Thread.sleep(intervalBetweenRetries);
		    		continue;
		    	}
		    	else
		    	{
		    		token = loginRetType.getSecurityToken().getValue();
			    	logger.info("successfully retrieved the token");
			    	attemptFlag = false;
		    	}
		    	
			}
			catch (Exception e)
			{
				logger.error("Following exception encountered while trying to login");
				logger.error(e.getMessage(),e);
				retryAttempt++;
				if (retryAttempt <= noOfRetryAttempts)
				{
					logger.info("Calling login service with retry attempt "+retryAttempt);
					try {
						Thread.sleep(intervalBetweenRetries);
					} catch (InterruptedException e1) {
						throw new AMSSServiceException(e.getMessage());
					}
				}
			}
		}
		
		if (attemptFlag)
    	{
			retryAttempt--;
    		logger.error("Calling login service failed after "+retryAttempt+" retry attempts");
    		throw new AMSSServiceException("Calling login service failed after "+retryAttempt+" retry attempts");
    	}
		
		return token;
    	
    	
	}
	

	public ImplWSOrderService getImplOrderService() {
		return implOrderService;
	}

	public void setImplOrderService(ImplWSOrderService implOrderService) {
		this.implOrderService = implOrderService;
	}

	public SecurityTokenServiceSvcService getSecService() {
		return secService;
	}


	public void setSecService(SecurityTokenServiceSvcService secService) {
		this.secService = secService;
	}

	public static void main( String[] args ) throws JAXBException, AMSSServiceException, MalformedURLException
    {
//    	AMSSClientImpl client = new AMSSClientImpl("http://cvlqepcsewl1.lab.cscqa.com:40150/Implws/services/ImplWSOrder", "http://cvlqepcsewl1.lab.cscqa.com:40150/sts/services/SecurityTokenServiceSvc");
//    	String token = client.login("webOrderUser", "Unix11");
    	String userId = "webOrderUser";
    	String password = "Unix11";
    	Set<String> cartIdList = new HashSet<String>();
    	cartIdList.add("NAUAY87S");
    	cartIdList.add("XBO4AJBQ");
    	cartIdList.add("CSCF2ISD");
    	//,CSCF2ISD,NAUAY87S
//    	client.retrieveCartProductListInfo("", userId, password,  cartIdList, "en_US", "WEB");
//    	JAXBContext ctx = JAXBContext.newInstance(RetrieveShoppingCartResponse.class);
//    	RetrieveShoppingCartResponse response = new RetrieveShoppingCartResponse();
//    	JAXBElement element = new JAXBElement(new QName("http://ws.ejb.order.impl.services.amdocs.com"), 
//    			RetrieveShoppingCartOutput.class, client.retrieveCartProductListInfo(userId, password, "XBO4AJBQ", "en_US", "WEB"));
//    	response.setRetrieveShoppingCartReturn(element);
//    	Marshaller marshaller = ctx.createMarshaller();
//    	marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
//    	marshaller.setProperty(Marshaller.JAXB_SCHEMA_LOCATION, "");
//    	marshaller.marshal(response, new File("shopping_cart_output4.xml"));
    	
    }
}
