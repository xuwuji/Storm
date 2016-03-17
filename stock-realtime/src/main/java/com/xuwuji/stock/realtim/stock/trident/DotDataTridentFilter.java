package com.xuwuji.stock.realtim.stock.trident;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.json.simple.JSONValue;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class DotDataTridentFilter extends BaseFilter {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOGGER = Logger.getLogger(DotDataTridentFilter.class);

	public boolean isKeep(TridentTuple tuple) {
		HashMap<String, Object> map = (HashMap<String, Object>) JSONValue.parse(String.valueOf(tuple.get(0)));
		for (Entry<String, Object> entry : map.entrySet()) {
			LOGGER.error(entry.getKey());
			LOGGER.error(entry.getValue());
		}
		return false;
	}

}
