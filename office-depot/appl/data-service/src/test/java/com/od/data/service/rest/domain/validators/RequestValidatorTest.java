package com.od.data.service.rest.domain.validators;

import java.util.ArrayList;

import javax.validation.ConstraintValidatorContext;
import javax.validation.ConstraintValidatorContext.ConstraintViolationBuilder;

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
import com.od.data.service.rest.domain.model.ErrorCode;
import com.od.data.service.rest.domain.validators.RequestValidator;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@WebAppConfiguration
public class RequestValidatorTest {

  private RequestValidator requestValidator;
  private ConstraintValidatorContext contextMock;

  @Before
  public void setUp() throws Exception {
    requestValidator = new RequestValidator();
    contextMock = Mockito.mock(ConstraintValidatorContext.class);
  }

  @Test
  public void testIsValidWhenDateIsEmpty() {
    ClickStreamRequest clickStreamRequest = prepareRequest();
    clickStreamRequest.setDimensions("country");
    clickStreamRequest.setMetrics("totalOrder");
    clickStreamRequest.setToDate("");
    clickStreamRequest.setFromDate("");
    ConstraintViolationBuilder constraintViolationBuilderMock = Mockito.mock(ConstraintViolationBuilder.class);
    Mockito.when(contextMock.buildConstraintViolationWithTemplate(ErrorCode.INVALID_DATE_FORMAT.name())).thenReturn(
        constraintViolationBuilderMock);

    boolean Isvalid = requestValidator.isValid(clickStreamRequest, contextMock);

    Assert.assertFalse(Isvalid);
  }

  private ClickStreamRequest prepareRequest() {
    ClickStreamRequest clickStreamRequest = new ClickStreamRequest();
    clickStreamRequest.setToDate("2014'Q'10");
    clickStreamRequest.setFromDate("2014'Q'01");
    ArrayList<String> filterList = new ArrayList<String>();
    filterList.add("country");
    clickStreamRequest.setFilters(filterList);
    return clickStreamRequest;
  }
}