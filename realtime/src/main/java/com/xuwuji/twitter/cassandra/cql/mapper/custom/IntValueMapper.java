package com.xuwuji.twitter.cassandra.cql.mapper.custom;

import com.datastax.driver.core.Row;
import com.xuwuji.twitter.cassandra.cql.mapper.base.BaseRowMapper;

public class IntValueMapper extends BaseRowMapper<Integer> {

	private static final long serialVersionUID = 1L;

	public IntValueMapper(String keyspace, String table, String[] keyColumns, String[] valueColumns) {
		super(keyspace, table, keyColumns, valueColumns);
	}

	@Override
	public Integer getValue(Row row) {
		return row.getInt("count");
	}

}
