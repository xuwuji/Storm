package com.xuwuji.twitter.cassandra.cql.mapper.custom;

import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.xuwuji.twitter.cassandra.cql.mapper.base.BaseRowMapper;

public class GeoMapper extends BaseRowMapper<Object> {

	private static final long serialVersionUID = 1L;

	public GeoMapper(String keyspace, String table, String[] columnsNames) {
		super(keyspace, table, columnsNames);
	}

	@Override
	public Object getValue(Row row) {
		return null;
	}

	@Override
	public Statement map(List<Object> key, Object value) {
		return null;
	}

	@Override
	public Statement retrieve(List<Object> key) {
		return null;
	}

}
