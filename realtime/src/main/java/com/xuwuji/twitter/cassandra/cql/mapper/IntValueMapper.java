package com.xuwuji.twitter.cassandra.cql.mapper;

import com.datastax.driver.core.Row;

public class IntValueMapper extends BaseRowMapper<Integer> {

	private static final long serialVersionUID = 1L;

	public IntValueMapper(String keyspace, String table, String[] keyColumns, String[] valueColumns) {
		super(keyspace, table, keyColumns, valueColumns);
	}

	@Override
	public Integer getValue(Row row) {
		return row.getInt(this.columnsNames[columnsNames.length - 1]);
	}

}
