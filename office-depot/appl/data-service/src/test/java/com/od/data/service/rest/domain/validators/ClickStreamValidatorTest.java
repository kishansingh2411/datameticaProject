package com.od.data.service.rest.domain.validators;

import javax.validation.ConstraintValidatorContext;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Module;
import com.od.data.service.rest.domain.validators.RequestValidator;

/**
 * @author sandeep
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class ClickStreamValidatorTest {

  private RequestValidator clickStrreamValidator;
  private ConstraintValidatorContext context;

  @Before
  public void setup() {
    clickStrreamValidator = new RequestValidator();
    context = Mockito.mock(ConstraintValidatorContext.class);
  }

  @Test
  public void testIsValidForSortIsNull() {
    ClickStreamRequest clickStreamRequest = Mockito.mock(ClickStreamRequest.class);
    Mockito.when(clickStreamRequest.getDimensions()).thenReturn("country,visitorSegment");
    Mockito.when(clickStreamRequest.getMetrics()).thenReturn("totalRevenue,totalOrder,totalVisit");
    Mockito.when(clickStreamRequest.getFromDate()).thenReturn("2014-08-20");
    Mockito.when(clickStreamRequest.getToDate()).thenReturn("2014-08-22");
    Mockito.when(clickStreamRequest.getModule()).thenReturn(Module.CLICKSTREAM);

    Boolean isValid = clickStrreamValidator.isValid(clickStreamRequest, context);
    Assert.assertTrue(isValid);
  }
}
