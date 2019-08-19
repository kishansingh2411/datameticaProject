package com.od.data.service.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.PropertySource;

/**
 * @author sandeep
 *
 */
@Profile("qa")
@Configuration
@PropertySource("classpath:application-qa.properties")
public class TestConfig {

}
