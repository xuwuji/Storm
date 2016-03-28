package com.xuwuji.realtime.util;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

/**
 * Using kafka as the spout. create a kafka spout based on config.
 * 
 * @author wuxu
 *
 */
public class KafkaSpoutFactory {

	/**
	 * 
	 * @param host
	 *            zookeeper host
	 * @param topic
	 *            which topic to read messages from kafka
	 * @param root
	 *            Where to store state in ZK (don't change this)
	 * @param id
	 *            Unique id of this spout. This needs to be unique across ALL
	 *            topologies.
	 * @return
	 */
	public static KafkaSpout createSpout(String host, String topic, String root, String id) {
		SpoutConfig config = new SpoutConfig(new ZkHosts(host), topic, root, id);
		// The data our application writes to Kafka is a simple Java string, so
		// we use Storm- Kafka built-in StringScheme class. The StringScheme
		// class will read data from Kafka as a string and output it in a tuple
		// field named str.
		config.scheme = new SchemeAsMultiScheme(new StringScheme());
		// KafkaSpout extends BaseRichSpout, so it takes the config as a
		// parameter and send tuples from the kafka source
		return new KafkaSpout(config);
	}

	public static OpaqueTridentKafkaSpout createTridentSpout(String host, String topic, String id,
			boolean startFromBeginning) {
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(new ZkHosts(host), topic, id);
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		if (!startFromBeginning) {
			spoutConf.startOffsetTime = -1;// start from latest offset
		} else {
			System.out.println("start from earliest, force from start");
			spoutConf.forceFromStart = true;
			spoutConf.startOffsetTime = -2; // start from the earliest
		}
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		return spout;
	}
}
