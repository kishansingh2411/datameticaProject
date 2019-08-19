package com.cablevision.shoppingcart;

import java.util.List;

public class CartProductListInfo {
	
	private String cartId;
	List<RateCodeInfo> rateCodeList;
	List<ProductOfferingInfo> productOfferingList;
	
	public String getCartId() {
		return cartId;
	}
	public void setCartId(String cartId) {
		this.cartId = cartId;
	}
	
	public List<RateCodeInfo> getRateCodeList() {
		return rateCodeList;
	}
	public void setRateCodeList(List<RateCodeInfo> rateCodeList) {
		this.rateCodeList = rateCodeList;
	}
	
	public List<ProductOfferingInfo> getProductOfferingList() {
		return productOfferingList;
	}
	public void setProductOfferingList(List<ProductOfferingInfo> productOfferingList) {
		this.productOfferingList = productOfferingList;
	}
	
	
}
