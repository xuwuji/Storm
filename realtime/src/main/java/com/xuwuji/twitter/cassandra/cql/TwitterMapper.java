package com.xuwuji.twitter.cassandra.cql;

import java.io.Serializable;
import java.util.List;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;

import storm.trident.tuple.TridentTuple;

public class TwitterMapper implements Serializable {

	private static final long serialVersionUID = 1L;

}
