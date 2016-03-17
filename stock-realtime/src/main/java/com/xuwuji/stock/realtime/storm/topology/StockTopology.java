package com.xuwuji.stock.realtime.storm.topology;

import com.xuwuji.stock.realtime.storm.bolt.DotDataBoltFilter;
import com.xuwuji.stock.realtime.storm.bolt.MovingAverageBolt;
import com.xuwuji.stock.realtime.storm.spout.KafkaSpoutFactory;
import com.xuwuji.stock.realtime.util.Constants;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;

public class StockTopology {

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("stock_spout", KafkaSpoutFactory.createSpout(Constants.ZKHOST, Constants.STOCK_TOPIC,
				"/kafka/stock", "kafka.stock.dot.spout"));
		builder.setBolt("stock_dot_filter", new DotDataBoltFilter()).shuffleGrouping("stock_spout");
		builder.setBolt("MovingAverage_bolt", new MovingAverageBolt()).shuffleGrouping("stock_dot_filter");
		Config config = new Config();

		LocalCluster cluster = new LocalCluster();

		cluster.submitTopology("stock-topology", config, builder.createTopology());
		// waitForSeconds(10);
		// cluster.killTopology("stock-topology");
		// cluster.shutdown();
	}

}
