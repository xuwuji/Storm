package wordcount;

import java.util.Map;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import utils.Utils;

public class SentenceSpout extends BaseRichSpout

{
	private SpoutOutputCollector collector;

	private String[] list = { "aa bb cc", "aa bb cc", "aa bb cc", "aa bb cc", "aa bb cc" };

	private int index = 0;

	public void nextTuple() {
		this.collector.emit(new Values(list[index]));
		index++;
		if (index >= list.length) {
			index = 0;
		}
		Utils.waitForMillis(1);
	}

	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("sentence"));

	}

}
