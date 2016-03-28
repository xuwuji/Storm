package com.xuwuji.twitter.storm.topology;

import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.KafkaSpoutFactory;
import com.xuwuji.stock.model.Tweet;
import com.xuwuji.stock.trident.operation.LogHandler;
import com.xuwuji.twitter.storm.trident.operation.TwitterParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class TwitterTopology {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("str", KafkaSpoutFactory.createTridentSpout(Constants.ZKHOST,
				Constants.TWITTER_TOPIC, "twitter-spout", false));
		String[] fields = new String[] { Tweet.TIME, Tweet.USERNAME, Tweet.LOCATION, Tweet.TEXT, Tweet.TAGS };
		Stream parsedStream = stream.each(new Fields("str"), new TwitterParser(fields), new Fields(fields))
				.each(new Fields(fields), new LogHandler());
		// The project method on Stream keeps only the fields specified in
		// the operation.
		parsedStream = parsedStream.project(new Fields(fields));
		LocalCluster cluster = new LocalCluster();
		Config config = new Config();
		cluster.submitTopology("twitter-trident-topology", config, topology.build());
	}
}
