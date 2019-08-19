package com.od.clickstream.listener;

import com.od.clickstream.constants.Constants;
import com.od.clickstream.facade.FlumeFacade;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

/**
 * @author sandeep
 */
public class FlumeListener implements ServletContextListener {
  
  private FlumeFacade client;
  
  public void contextDestroyed(final ServletContextEvent servletContextEvent) {
    client.cleanUp();
    final ServletContext context = servletContextEvent.getServletContext();
    context.removeAttribute(Constants.ATTR_FLUME);
  }

  public void contextInitialized(final ServletContextEvent servletContextEvent) {
    client = new FlumeFacade();
    client.init(Constants.HOST_LOCALHOST, Constants.FLUME_RPC_PORT);
    final ServletContext context = servletContextEvent.getServletContext();
    context.setAttribute(Constants.ATTR_FLUME, client);
  }
}
