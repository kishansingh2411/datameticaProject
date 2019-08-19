package com.od.data.service.config;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.ResourceBundleMessageSource;
import org.springframework.core.env.Environment;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;

/**
 * @author sandeep
 *
 */
@Configuration
@EnableWebMvc
@ComponentScan(basePackages = "com.od.data.service")
public class RESTConfig {

  @Autowired
  private Environment environment;

  @Bean
  public ResourceBundleMessageSource getMessageSource() {
    ResourceBundleMessageSource messageSource = new ResourceBundleMessageSource();
    messageSource.setBasename("i18n/messages");
    messageSource.setUseCodeAsDefaultMessage(Boolean.TRUE);

    return messageSource;
  }

  @Bean
  public DataSource getDataSource() {
    BasicDataSource dataSource = new BasicDataSource();
    dataSource.setDriverClassName(environment.getProperty("phoenix.jdbc.driver.class"));
    dataSource.setUrl(environment.getProperty("phoenix.jdbc.url"));
    dataSource.setInitialSize(8);
    dataSource.setMaxActive(65);
    dataSource.setMaxIdle(18);

    return dataSource;
  }
}
