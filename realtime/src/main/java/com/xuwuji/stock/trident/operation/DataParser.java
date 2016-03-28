package com.xuwuji.stock.trident.operation;

import java.util.HashMap;

import org.json.simple.JSONValue;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class DataParser extends BaseFunction {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private String[] tags;

	public DataParser(String[] tags) {
		this.tags = tags;
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		HashMap<String, Object> map = new HashMap<String, Object>();
		map = (HashMap<String, Object>) JSONValue.parse(str);
		Values values = new Values();
		for (String tag : tags) {
			values.add(map.get(tag));
		}
		collector.emit(values);
	}

}
