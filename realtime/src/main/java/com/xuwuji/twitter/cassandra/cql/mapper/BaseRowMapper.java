package com.xuwuji.twitter.cassandra.cql.mapper;

import java.io.Serializable;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

import storm.trident.tuple.TridentTuple;

/**
 * It is the base row class mapper. It implements the CqlRowMapper and
 * CqlTupleMapper.
 * 
 * It initializes the connection with the Cassandra DB. Write the logic when
 * overriding the methods.
 * 
 * @author wuxu 2016-3-28
 *
 * @param <K>
 * @param <V>
 */
public class BaseRowMapper<V> implements CqlRowMapper<List<Object>, V>, Serializable {

	private static final long serialVersionUID = 1L;
	protected String keyspace;
	protected String table;
	protected String[] keyNames;
	protected String[] valueNames;
	protected String[] columnsNames;
	protected Object[] columnValues;

	public BaseRowMapper(String keyspace, String table, String[] keyColumns, String[] valueColumns) {
		this.table = table;
		this.keyspace = keyspace;
		// names for the key columns
		this.keyNames = keyColumns;
		// names for the value columns
		this.valueNames = valueColumns;
		// all names for all columns(including the key and value)
		this.columnsNames = new String[keyColumns.length + valueColumns.length];
		// all values for all columns(including the key and value)
		this.columnValues = new Object[keyColumns.length + valueColumns.length];
		// copy keys' names and values' into columsNames array
		System.arraycopy(keyNames, 0, columnsNames, 0, keyNames.length);
		System.arraycopy(valueNames, 0, columnsNames, keyNames.length, valueNames.length);
	}

	@Override
	public V getValue(Row row) {
		// TODO Auto-generated method stub
		return null;
	}

	/**
	 * insert column and its corresponding value into the given keyspace and
	 * table
	 */
	@Override
	public Statement map(List<Object> key, V value) {
		Insert statement = QueryBuilder.insertInto(keyspace, table);
		Object[] mappedKey = key.toArray();
		Object[] mappedValue = new Object[] { value };
		// copy the key columns into the columnValues array
		System.arraycopy(mappedKey, 0, columnValues, 0, mappedKey.length);
		// copy the value columns into the columnValues array
		System.arraycopy(mappedValue, 0, columnValues, mappedKey.length, mappedValue.length);
		// insert into the db
		statement.values(columnsNames, columnValues);
		return statement;
	}

	/**
	 * insert tuples into db
	 */
	@Override
	public Statement map(TridentTuple tuple) {
		Insert statement = QueryBuilder.insertInto(keyspace, table);
		statement.values(columnsNames, tuple.toArray());
		return statement;
	}

	@Override
	public Statement retrieve(List<Object> key) {
		Select statement = QueryBuilder.select(columnsNames).from(keyspace, table);
		Object[] mappedKey = key.toArray();
		for (int i = 0; i < mappedKey.length; i++) {
			statement.where(QueryBuilder.eq(columnsNames[i], mappedKey[i]));
		}
		return statement;
	}

}
