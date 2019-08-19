package com.cablevision.edp;

import java.util.Date;

import com.cablevision.edp.JdbcSourceTaskConfig;

public class DDPTimestampIncrementingTableQuerier extends GenericTimestampIncrementingTableQuerier{

	public DDPTimestampIncrementingTableQuerier(QueryMode mode, String name, String topicPrefix, String timestampColumn,
			Long timestampOffset, String incrementingColumn, Long incrementingOffset,
			String additionalQueryConditionStr, String queryHint, JdbcSourceTaskConfig config, Date startLoadDate, String tableName) {
		super(mode, name, topicPrefix, timestampColumn, timestampOffset, incrementingColumn, incrementingOffset,
				additionalQueryConditionStr, queryHint, startLoadDate, tableName);
		
		
	}
	
	
	@Override
	protected String buildCustomQueryCondition() {

		StringBuilder queryCondition = new StringBuilder();
		queryCondition.append("LOAD_DATE >= ").append(" ? ").append(" AND ").append("LOAD_DATE < ").append(" ? ");
		return queryCondition.toString();
	}
	
	

}
