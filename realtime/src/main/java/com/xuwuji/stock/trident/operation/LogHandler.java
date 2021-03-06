package com.xuwuji.stock.trident.operation;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

public class LogHandler extends BaseFilter {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOGGER = Logger.getLogger(LogHandler.class);

	public boolean isKeep(TridentTuple tuple) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < tuple.size(); i++) {
			builder.append(String.valueOf(tuple.get(i)) + ";  ");
		}
		LOGGER.info(builder.toString());
		// System.out.println(builder.toString());
		return true;
	}

}
