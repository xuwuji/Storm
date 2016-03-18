package com.xuwuji.stock.realtim.stock.trident;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * 
 * @author wuxu 2016-3-18
 *
 *         this is used for getting hour of the day (0-23) and day of the
 *         week(1-7)
 */
public class TimeTridentParser extends BaseFunction {

	private String type;
	private static Logger LOGGER = Logger.getLogger(TimeTridentParser.class);

	public TimeTridentParser(String type) {
		this.type = type;
	}

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public void execute(TridentTuple tuple, TridentCollector collector) {
		DateTime time = new DateTime(tuple.getLong(0));
		Values values = new Values();
		switch (type) {
		case "HOUR":
			int hour = time.getHourOfDay();
			// LOGGER.error("-------" + hour);
			if (hour >= 9 && hour <= 15) {
				values.add(hour);
				collector.emit(values);
			}
			break;
		case "DAY":
			int day = time.getDayOfWeek();
			// LOGGER.error("-------" + day);
			if (day >= 1 && day <= 5) {
				values.add(day);
				collector.emit(values);
			}
			break;
		}
	}

}
