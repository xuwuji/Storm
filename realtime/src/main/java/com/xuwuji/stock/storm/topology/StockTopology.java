package com.xuwuji.stock.storm.topology;

import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.KafkaSpoutFactory;
import com.xuwuji.stock.storm.bolt.DotDataBoltFilter;
import com.xuwuji.stock.storm.bolt.MovingAverageBolt;

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
