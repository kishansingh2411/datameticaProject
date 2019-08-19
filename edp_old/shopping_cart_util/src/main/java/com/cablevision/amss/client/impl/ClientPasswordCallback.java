package com.cablevision.amss.client.impl;
import java.io.IOException;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;

import org.apache.wss4j.common.ext.WSPasswordCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
 
public class ClientPasswordCallback implements CallbackHandler {
 
	private static final Logger logger = LoggerFactory.getLogger(ClientPasswordCallback.class);
    public void handle(Callback[] callbacks) throws IOException, 
        UnsupportedCallbackException {
 
        WSPasswordCallback pc = (WSPasswordCallback) callbacks[0];
        
        logger.debug("Setting password ------->");
        // set the password for our message.
        pc.setPassword("abcdefg");
    }
 
}