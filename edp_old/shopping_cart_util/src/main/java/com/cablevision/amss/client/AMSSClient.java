package com.cablevision.amss.client;

import java.util.Set;

import com.cablevision.shoppingcart.ShoppingCartResult;

public interface AMSSClient {

	public ShoppingCartResult retrieveCartProductListInfo(String token, String userId, String password, Set<String> cartIdList, String locale, String salesChannel)
					throws AMSSServiceException;

	String login(String userId, String password) throws AMSSServiceException;
	
}
