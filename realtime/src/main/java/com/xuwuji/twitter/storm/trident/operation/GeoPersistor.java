package com.xuwuji.twitter.storm.trident.operation;

import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class GeoPersistor extends BaseFunction {

	private static final long serialVersionUID = 1L;
	private String keyspace;
	private String table;
	private String[] columnNames;

	public GeoPersistor(String keyspace, String table, String[] columnNames) {
		this.keyspace = keyspace;
		this.table = table;
		this.columnNames = columnNames;
	}

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// System.out.println("!!!!!!!!!!!");
		if (tuple.size() != 0) {
			System.out.println("++++");
			Insert statement = QueryBuilder.insertInto(keyspace, table);
			statement.values(columnNames, tuple.toArray());
		}
	}

}
