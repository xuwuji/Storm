package com.xuwuji.twitter.storm.state;

import java.io.Serializable;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import com.hmsonline.trident.cql.CassandraCqlMapStateFactory;
import com.hmsonline.trident.cql.CassandraCqlMapState;
import com.hmsonline.trident.cql.mappers.CqlRowMapper;
import com.xuwuji.twitter.cassandra.cql.mapper.BaseRowMapper;

import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

/**
 * it is used for getting state factory
 * 
 * @author wuxu 2016-3-28
 *
 */
public class TwitterPersistManager implements Serializable {

	private static final long serialVersionUID = 1L;
	// options for the cassandra cql state
	@SuppressWarnings("rawtypes")
	private CassandraCqlMapState.Options options;
	private Map<String, Object> config;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public TwitterPersistManager(Map<String, Object> config) {
		this.config = config;
		options = new CassandraCqlMapState.Options();
		this.options.keyspace = (String) config.get("keyspace");
		this.options.tableName = (String) config.get("tablename");
		this.options.maxBatchSize = (Integer) config.get("batchsize");
	}

	/**
	 * 
	 * @param keys
	 *            represents which columns are the keys
	 * @param metrics
	 *            represents which columns are the values
	 * @param mapperClass
	 *            mapper class does the real logic
	 * @param type
	 *            state type
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public StateFactory getState(String keys[], String metrics[], Class mapperClass, StateType type) {
		try {
			Constructor constructor = mapperClass.getConstructor(String.class, String.class, String[].class,
					String[].class);
			String keyspace = (String) config.get("keyspace");
			String tablename = (String) config.get("tablename");
			// initialize a new mapper class
			BaseRowMapper mapper = (BaseRowMapper) constructor.newInstance(keyspace, tablename, keys, metrics);
			return createStateFactory(mapper, type);
		} catch (Exception e) {
			throw new RuntimeException("Mapper class not found:" + mapperClass.getCanonicalName(), e);
		}
	}

	@SuppressWarnings("rawtypes")
	private StateFactory createStateFactory(CqlRowMapper mapper, StateType type) {
		return new CassandraCqlMapStateFactory(mapper, type, options);

	}

}
