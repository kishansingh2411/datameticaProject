package com.od.data.service.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.od.data.service.dao.BaseDao;
import com.od.data.service.rest.domain.ClickStreamRequest;
import com.od.data.service.rest.domain.Response;
import com.od.data.service.service.ClickStreamService;
import com.od.data.service.service.QueryBuilder;

/**
 * @author sandeep
 * 
 */
@Service("clickStreamService")
public class ClickStreamServiceImpl extends AbstractServiceImpl implements ClickStreamService {

  @Override
  public Response getRecordsForClickStream(ClickStreamRequest clickStreamRequest) {
    return getRecords(clickStreamRequest);
  }

  @Autowired
  public void setQueryBuilder(QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  @Autowired
  public void setClickStreamDao(BaseDao clickStreamDao) {
    this.dao = clickStreamDao;
  }

}
