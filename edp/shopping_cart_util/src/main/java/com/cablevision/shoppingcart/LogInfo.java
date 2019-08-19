package com.cablevision.shoppingcart;

import java.math.BigDecimal;

public class LogInfo {
	
	private String sequenceId;
	private BigDecimal keyParamId;
	private BigDecimal h_shoppingCartIdBegin;
	private BigDecimal h_shoppingCartIdEnd;
	private long cartIdTotalRequesCount;
	private long cartIdSuccessRequestCount;
	private long cartIdNoResponseCount;
	private java.sql.Timestamp dtmStartTime;
	private java.sql.Timestamp dtmEndTime;
	private java.sql.Timestamp dtmCreatedTime;
	
	public String getSequenceId() {
		return sequenceId;
	}
	public void setSequenceId(String sequenceId) {
		this.sequenceId = sequenceId;
	}
	public BigDecimal getKeyParamId() {
		return keyParamId;
	}
	public void setKeyParamId(BigDecimal keyParamId) {
		this.keyParamId = keyParamId;
	}
	public BigDecimal getH_shoppingCartIdBegin() {
		return h_shoppingCartIdBegin;
	}
	public void setH_shoppingCartIdBegin(BigDecimal h_shoppingCartIdBegin) {
		this.h_shoppingCartIdBegin = h_shoppingCartIdBegin;
	}
	public BigDecimal getH_shoppingCartIdEnd() {
		return h_shoppingCartIdEnd;
	}
	public void setH_shoppingCartIdEnd(BigDecimal h_shoppingCartIdEnd) {
		this.h_shoppingCartIdEnd = h_shoppingCartIdEnd;
	}
	public long getCartIdTotalRequesCount() {
		return cartIdTotalRequesCount;
	}
	public void setCartIdTotalRequesCount(long cartIdTotalRequesCount) {
		this.cartIdTotalRequesCount = cartIdTotalRequesCount;
	}
	public long getCartIdSuccessRequestCount() {
		return cartIdSuccessRequestCount;
	}
	public void setCartIdSuccessRequestCount(long cartIdSuccessRequestCount) {
		this.cartIdSuccessRequestCount = cartIdSuccessRequestCount;
	}
	public long getCartIdNoResponseCount() {
		return cartIdNoResponseCount;
	}
	public void setCartIdNoResponseCount(long cartIdNoResponseCount) {
		this.cartIdNoResponseCount = cartIdNoResponseCount;
	}
	public java.sql.Timestamp getDtmStartTime() {
		return dtmStartTime;
	}
	public void setDtmStartTime(java.sql.Timestamp dtmStartTime) {
		this.dtmStartTime = dtmStartTime;
	}
	public java.sql.Timestamp getDtmEndTime() {
		return dtmEndTime;
	}
	public void setDtmEndTime(java.sql.Timestamp dtmEndTime) {
		this.dtmEndTime = dtmEndTime;
	}
	public java.sql.Timestamp getDtmCreatedTime() {
		return dtmCreatedTime;
	}
	public void setDtmCreatedTime(java.sql.Timestamp dtmCreated) {
		this.dtmCreatedTime = dtmCreated;
	}
}
