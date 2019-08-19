package com.od.data.service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;
import org.springframework.context.annotation.PropertySources;

/**
 * @author sandeep
 *
 */
@Profile("qa")
@Configuration
@PropertySources(value = {@PropertySource("classpath:application-qa.properties")})
public class QAConfig {

}
