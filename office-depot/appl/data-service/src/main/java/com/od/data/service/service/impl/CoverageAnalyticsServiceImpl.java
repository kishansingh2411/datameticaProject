package com.od.data.service.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.CoverageAnalyticsRequest;
//import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.CoverageAnalyticsService;
//import com.od.data.service.service.ClickStreamService;
//import com.od.data.service.service.CoverageAnalyticsService;
import com.od.data.service.service.QueryBuilder;

/**
 * @author Sonali Rawool
 * 
 */
@Service("coverageAnalyticsService")
public class CoverageAnalyticsServiceImpl extends AbstractServiceImpl implements CoverageAnalyticsService {

  @Override
  public Response getRecordsForCoverageAnalytics(CoverageAnalyticsRequest request) {
    return getRecords(request);
  }

  @Autowired
  public void setQueryBuilder(QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  @Autowired
  public void setCoverageAnalyticsDao(BaseDao coverageAnalyticsDao) {
    this.dao = coverageAnalyticsDao;
  }

}
