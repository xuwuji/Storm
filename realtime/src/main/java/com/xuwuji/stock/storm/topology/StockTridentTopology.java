package com.xuwuji.stock.storm.topology;

import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.KafkaSpoutFactory;
import com.xuwuji.stock.model.Stock;
import com.xuwuji.stock.trident.operation.DataParser;
import com.xuwuji.stock.trident.operation.DotDataTridentFilter;
import com.xuwuji.stock.trident.operation.LogHandler;
import com.xuwuji.stock.trident.operation.TimeTridentParser;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.Fields;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;

public class StockTridentTopology {

	public static void main(String[] args) {
		TridentTopology topology = new TridentTopology();
		Stream stream = topology.newStream("str", KafkaSpoutFactory.createTridentSpout(Constants.ZKHOST,
				Constants.STOCK_TOPIC, "stock-trident-spout", true));
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
				.each(new Fields(Stock.SHANGHAI, Stock.SHENZHEN, Stock.HSI, Stock.DJI, "hour", "day"),
						new LogHandler());

		GroupedStream DayStream = validStream.groupBy(new Fields("day"));

		Config config = new Config();
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("stock-trident-topology", config, topology.build());
	}
}
