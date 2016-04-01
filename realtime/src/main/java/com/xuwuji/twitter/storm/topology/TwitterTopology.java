package com.xuwuji.twitter.storm.topology;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.ConsistencyLevel;
import com.hmsonline.trident.cql.CassandraCqlStateFactory;
import com.hmsonline.trident.cql.CassandraCqlStateUpdater;
import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.KafkaSpoutFactory;
import com.xuwuji.realtime.util.TimeType;
import com.xuwuji.stock.model.Tweet;
import com.xuwuji.twitter.cassandra.cql.mapper.custom.GeoMapper;
import com.xuwuji.twitter.cassandra.cql.mapper.custom.IntValueMapper;
import com.xuwuji.twitter.storm.state.TwitterPersistManager;
import com.xuwuji.twitter.storm.trident.operation.Count;
import com.xuwuji.twitter.storm.trident.operation.GeoParser;
import com.xuwuji.twitter.storm.trident.operation.TimeRound;
import com.xuwuji.twitter.storm.trident.operation.TweetParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

public class TwitterTopology {

	private static Fields COUNT = new Fields("count");

	public StormTopology build() {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("twitter-location-topology", KafkaSpoutFactory
				.createTridentSpout(Constants.ZKHOST, Constants.TWITTER_TOPIC, "twitter-spout", false));
		String[] fields = new String[] { Tweet.TIME, Tweet.USERNAME, Tweet.LOCATION, Tweet.TEXT, Tweet.TAGS };
		String[] parsedfields = new String[] { Tweet.TIME, Tweet.USERNAME, Tweet.LOCATION, Tweet.TEXT, "tag" };

		// 1. parse the stream into two types: separate and non-separate
		Stream separateParsedStream = stream
				.each(new Fields("str"), new TweetParser(fields, true), new Fields(parsedfields))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.HOUR), new Fields("hour"))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.DAY), new Fields("day"))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.MONTH), new Fields("month"));

		Stream nonseparateParsedStream = stream
				.each(new Fields("str"), new TweetParser(fields, false), new Fields(parsedfields))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.HOUR), new Fields("hour"))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.DAY), new Fields("day"))
				.each(new Fields(Tweet.TIME), new TimeRound(TimeType.MONTH), new Fields("month"));

		// 2. get detailed stream based on different dimensions
		Stream locationStream = nonseparateParsedStream.project(new Fields(Tweet.LOCATION, "hour", "day", "month"));
		Stream tagStream = separateParsedStream.project(new Fields("tag", "hour", "day", "month"));
		Stream geoStream = nonseparateParsedStream.project(new Fields(Tweet.LOCATION, "hour", "day", "month"))
				.each(new Fields(Tweet.LOCATION), new GeoParser(), new Fields("lat", "lng"));

		// declare the config for the cassandra persistence manager
		Map<String, Object> persistConfig = new HashMap<String, Object>();
		persistConfig.put("keyspace", "mykeyspace");
		persistConfig.put("batchsize", 10);
		TwitterPersistManager manager = new TwitterPersistManager(persistConfig);

		String[] locationKeys = new String[] { Tweet.LOCATION, "hour", "day", "month" };
		String[] tagKeys = new String[] { "tag", "hour", "day", "month" };

		StateFactory locationState = manager.getState(locationKeys, new String[] { "count" }, IntValueMapper.class,
				StateType.NON_TRANSACTIONAL, "location");
		StateFactory tagState = manager.getState(tagKeys, new String[] { "count" }, IntValueMapper.class,
				StateType.NON_TRANSACTIONAL, "tag");

		// remember the stream should be grouped by the keys, otherwise it got
		// an error
		groupByLocation(locationStream, locationState, locationKeys);
		groupByTag(tagStream, tagState, tagKeys);

		// persist each geo message into cassandra
		persistGeo(geoStream);
		return topology.build();
	}

	public void groupByLocation(Stream stream, StateFactory state, String[] keys) {
		stream.groupBy(new Fields(Arrays.asList(keys))).persistentAggregate(state, new Count(), COUNT)
				.parallelismHint(2);
	}

	public void groupByTag(Stream stream, StateFactory state, String[] keys) {
		stream.groupBy(new Fields(Arrays.asList(keys))).persistentAggregate(state, new Count(), COUNT)
				.parallelismHint(2);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void persistGeo(Stream stream) {
		String[] geoColumns = new String[] { "lat", "lng", "hour", "day", "month" };
		stream.partitionPersist(new CassandraCqlStateFactory(ConsistencyLevel.ONE), new Fields(geoColumns),
				// if set the mapper to false, the tuple won't emit any more
				new CassandraCqlStateUpdater(new GeoMapper("mykeyspace", "geo", geoColumns), false));
	}

	public static void main(String[] args) {
		String env = "local";
		if (args.length != 0) {
			env = args[0];
		}
		TwitterTopology topology = new TwitterTopology();
		Config config = new Config();
		if (env.equals("local")) {
			// set the host for cassandra,run in local mode
			config.put(MapConfiguredCqlClientFactory.TRIDENT_CASSANDRA_CQL_HOSTS, "localhost");
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("twitter-trident-topology", config, topology.build());
		} else {
			// set config for cluster mode
		}

	}
}
