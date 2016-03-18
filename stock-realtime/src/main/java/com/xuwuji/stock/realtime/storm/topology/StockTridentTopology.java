package com.xuwuji.stock.realtime.storm.topology;

import com.xuwuji.stock.realtim.stock.trident.DataParser;
import com.xuwuji.stock.realtim.stock.trident.DotDataTridentFilter;
import com.xuwuji.stock.realtim.stock.trident.LogHandler;
import com.xuwuji.stock.realtim.stock.trident.TimeTridentParser;
import com.xuwuji.stock.realtime.model.Stock;
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
		String[] tags = new String[] { Stock.TIMESTAMP, Stock.SHANGHAI, Stock.SHENZHEN, Stock.HSI, Stock.DJI };
		Fields fields = new Fields(tags);
		// in trident, after each function, the data to be emitted is not
		// declared in the declare method like bolt, but declare as the third
		// parameter as following

		// 1.check every tag is not missing
		Stream validStream = stream.each(new Fields("str"), new DotDataTridentFilter())
				// 2.parse the message, extract each value, the order is set in
				// the tags
				.each(new Fields("str"), new DataParser(tags), fields)
				// 3. filter the message, it should between 9am and 3pm
				.each(new Fields(Stock.TIMESTAMP), new TimeTridentParser("HOUR"), new Fields("hour"))
				// 4. filter the message, it should between Monday and Friday
				.each(new Fields(Stock.TIMESTAMP), new TimeTridentParser("DAY"), new Fields("day"))
				.each(new Fields(Stock.SHANGHAI, Stock.SHENZHEN, Stock.HSI, Stock.DJI, "hour", "day"), new LogHandler(),
						new Fields("a"));

		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("stock-trident-topology", config, topology.build());
	}
}
