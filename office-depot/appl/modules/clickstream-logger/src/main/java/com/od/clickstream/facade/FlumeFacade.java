package com.od.clickstream.facade;

import com.google.common.base.Charsets;
import com.od.clickstream.constants.Constants;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;

/**
 * @author sandeep
 */
public class FlumeFacade {

  private RpcClient client;
  private String hostname;
  private int port;

  public void init(final String hostname, final int port)  {
    this.hostname = hostname;
    this.port = port;
    this.client = RpcClientFactory.getDefaultInstance(hostname, port);
  }

  public void sendDataToFlume(final String data) {
    final Event event = EventBuilder.withBody(data, Charsets.UTF_8);
    try {
    	if(this.client==null)
    	{
    		this.client=RpcClientFactory.getDefaultInstance(Constants.HOST_LOCALHOST,Constants.FLUME_RPC_PORT);
    	}
    	
      client.append(event);
    } catch (EventDeliveryException e) {
      e.printStackTrace();
      client.close();
      client = null;
      client = RpcClientFactory.getDefaultInstance(hostname, port);
    }
  }

  public void cleanUp() {
    client.close();
  }

}
