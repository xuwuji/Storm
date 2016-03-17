package com.xuwuji.stock.realtime.storm.topology;

import com.xuwuji.stock.realtim.stock.trident.DotDataTridentFilter;
import com.xuwuji.stock.realtime.storm.spout.KafkaSpoutFactory;
import com.xuwuji.stock.realtime.util.Constants;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;

public class StockTridentTopology {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("str",
				KafkaSpoutFactory.createTridentSpout(Constants.ZKHOST, Constants.STOCK_TOPIC, "stock-trident-spout"));
		stream.each(new Fields("str"), new DotDataTridentFilter());

		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("stock-trident-topology", config, topology.build());
	}
}
