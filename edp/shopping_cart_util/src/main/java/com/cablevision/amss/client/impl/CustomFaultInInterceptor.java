package com.cablevision.amss.client.impl;

import java.util.Collection;
import java.util.Set;

import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.PhaseInterceptor;

public class CustomFaultInInterceptor implements org.apache.cxf.interceptor.Interceptor<Message>, PhaseInterceptor<Message>{

	@Override
	public void handleFault(Message arg0) {
		
		System.out.println("Soap fault encountered");
		System.out.println("Fault message "+arg0.getExchange().getInFaultMessage());
		
	}

	@Override
	public void handleMessage(Message arg0) throws Fault {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Collection<PhaseInterceptor<? extends Message>> getAdditionalInterceptors() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getAfter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Set<String> getBefore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPhase() {
		// TODO Auto-generated method stub
		return null;
	}

	
	
}
