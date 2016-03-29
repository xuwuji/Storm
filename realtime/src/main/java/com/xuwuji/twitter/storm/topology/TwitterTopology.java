package com.xuwuji.twitter.storm.topology;

import java.util.HashMap;
import java.util.Map;

import com.hmsonline.trident.cql.MapConfiguredCqlClientFactory;
import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.KafkaSpoutFactory;
import com.xuwuji.stock.model.Tweet;
import com.xuwuji.stock.trident.operation.LogHandler;
import com.xuwuji.twitter.cassandra.cql.mapper.IntValueMapper;
import com.xuwuji.twitter.storm.state.TwitterPersistManager;
import com.xuwuji.twitter.storm.trident.operation.Count;
import com.xuwuji.twitter.storm.trident.operation.TwitterParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;

public class TwitterTopology {

	public StormTopology build() {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("twitter-location-topology", KafkaSpoutFactory
				.createTridentSpout(Constants.ZKHOST, Constants.TWITTER_TOPIC, "twitter-spout", false));
		String[] fields = new String[] { Tweet.TIME, Tweet.USERNAME, Tweet.LOCATION, Tweet.TEXT, Tweet.TAGS };
		Stream parsedStream = stream.each(new Fields("str"), new TwitterParser(fields), new Fields(fields))
				.each(new Fields(fields), new LogHandler());
		// The project method on Stream keeps only the fields specified in the
		// operation.
		Stream locationStream = parsedStream.project(new Fields(Tweet.LOCATION)).each(new Fields(Tweet.LOCATION),
				new LogHandler());

		// declare the config for the cassandra persistence manager
		Map<String, Object> persistConfig = new HashMap<String, Object>();
		persistConfig.put("keyspace", "mykeyspace");
		persistConfig.put("tablename", "twitterlocation");
		persistConfig.put("batchsize", 10);
		TwitterPersistManager manager = new TwitterPersistManager(persistConfig);

		StateFactory locationState = manager.getState(new String[] { Tweet.LOCATION }, new String[] { "count" },
				IntValueMapper.class, StateType.NON_TRANSACTIONAL);
		// group by location
		locationStream.groupBy(new Fields(Tweet.LOCATION))
				.persistentAggregate(locationState, new Count(), new Fields("count")).parallelismHint(2);
		return topology.build();
	}

	public static void main(String[] args) {
		String env = "local";
		if (args != null || args.length >= 2) {
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
