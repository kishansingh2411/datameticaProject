package com.od.data.service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

/**
 * @author sandeep
 *
 */
@Profile("prod")
@Configuration
@PropertySources(value = {@PropertySource("classpath:application-prod.properties")})
public class ProdConfig {

}
