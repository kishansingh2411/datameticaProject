package com.od.data.service.service.impl;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import com.od.data.service.config.DataAPIApplicationInitializer;
import com.od.data.service.config.RESTConfig;
import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.Query;
import com.od.data.service.rest.domain.Request;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.QueryBuilder;
import com.od.data.service.service.impl.AbstractServiceImpl;

/**
 * @author sandeep
 *
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles("qa")
@ContextConfiguration(classes = {DataAPIApplicationInitializer.class, RESTConfig.class})
@PrepareForTest({AbstractServiceImpl.class})
@WebAppConfiguration
public class AbstractServiceImplTest {

  private AbstractServiceImpl abstractServiceImpl;
  private QueryBuilder queryBuilderMock;
  private BaseDao baseDaoMock;

  @Before
  public void setUp() throws Exception {
    abstractServiceImpl = Mockito.spy(new AbstractServiceImpl());
    queryBuilderMock = Mockito.mock(QueryBuilder.class);
    abstractServiceImpl.queryBuilder = queryBuilderMock;
    baseDaoMock = Mockito.mock(BaseDao.class);
    abstractServiceImpl.dao = baseDaoMock;
  }

  @Test
  public void shouldGetJDBCQuery() {
    Query queryMock = Mockito.mock(Query.class);

    Request requestMock = Mockito.mock(Request.class);
    Mockito.doReturn(queryMock).when(abstractServiceImpl).getQueryObject(requestMock);

    String jdbcDummy = "Mock";
    Mockito.when(queryBuilderMock.buildQuery(queryMock)).thenReturn(jdbcDummy);

    Response response = abstractServiceImpl.getJDBCQuery(requestMock);

    Assert.assertSame(queryMock, response.getQuery());
    Assert.assertEquals(jdbcDummy, response.getPhoenixQuery());
  }

}
