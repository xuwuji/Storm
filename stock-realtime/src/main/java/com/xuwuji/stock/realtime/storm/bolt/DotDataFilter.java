package com.xuwuji.stock.realtime.storm.bolt;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import com.xuwuji.stock.realtime.model.Stock;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class DotDataFilter extends BaseBasicBolt {

	private static final long serialVersionUID = 1L;
	private static final Logger LOGGER = Logger.getLogger(DotDataFilter.class);

	public void execute(Tuple input, BasicOutputCollector collector) {
		String str = input.getString(0);
		HashMap<String, Object> map = (HashMap<String, Object>) JSONValue.parse(str);
		boolean tag = true;
		for (Entry<String, Object> entry : map.entrySet()) {
			String key = entry.getKey();
			Object value = entry.getValue();
			if (value == null || value.equals("") || value.equals("0")) {
				tag = false;
				LOGGER.error(key + " is empty");
			}
		}
		if (tag) {
			collector.emit(new Values(map.get("SH"), map.get("TIME")));
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("SH", "TIME"));
	}

}
