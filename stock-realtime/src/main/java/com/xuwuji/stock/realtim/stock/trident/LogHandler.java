package com.xuwuji.stock.realtim.stock.trident;

import org.apache.log4j.Logger;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class LogHandler extends BaseFunction {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static Logger LOGGER = Logger.getLogger(LogHandler.class);

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < tuple.size(); i++) {
			builder.append(String.valueOf(tuple.get(i)) + "; ");
		}
		LOGGER.error("-----" + builder.toString());

	}

}
