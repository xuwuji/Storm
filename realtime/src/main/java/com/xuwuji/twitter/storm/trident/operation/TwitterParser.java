package com.xuwuji.twitter.storm.trident.operation;

import java.util.HashMap;
import java.util.Map;

import org.json.simple.JSONValue;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * parse the tweets based on fields we want
 * 
 * @author wuxu 2016-3-21
 *
 */
public class TwitterParser extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private String[] fields;

	public TwitterParser(String[] fields) {
		this.fields = fields;
	}

	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		Map<String, Object> map = new HashMap<String, Object>();
		Values values = new Values();
		map = (Map<String, Object>) JSONValue.parse(str);
		boolean tag = true;
		for (String field : fields) {
			if (map.get(field) == null) {
				tag = false;
				break;
			} else {
				values.add(map.get(field));
			}
		}
		if (tag) {
			collector.emit(values);
		}
	}

}
