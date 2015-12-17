package wordcount;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import io.IOUtil;

public class ReportBolt extends BaseRichBolt {

	private HashMap<String, Long> map = null;

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = tuple.getLongByField("count");
		this.map.put(word, count);
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.map = new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	public void cleanup() {
		StringBuilder sb = new StringBuilder();
		System.out.println("\n\n\n-----------------------------------------");
		for (Entry<String, Long> entry : map.entrySet()) {
			sb.append(entry.getKey() + " : " + entry.getValue() + "\n");
			System.out.println(entry.getKey() + " : " + entry.getValue());
		}
		System.out.println("-----------------------------------------\n\n\n");
		try {
			IOUtil.log(sb.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
