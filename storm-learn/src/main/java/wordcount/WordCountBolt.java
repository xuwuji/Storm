package wordcount;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt extends BaseRichBolt {
	private OutputCollector collector;
	private HashMap<String, Long> map = null;

	public void execute(Tuple tuple) {
		String word = tuple.getStringByField("word");
		Long count = this.map.get(word);
		if (count == null) {
			count = 0L;
		}
		count++;
		this.map.put(word, count);
		this.collector.emit(new Values(word, count));
	}

	public void prepare(Map map, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.map = new HashMap<String, Long>();
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

}
