package com.xuwuji.twitter.storm.trident.operation;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import com.xuwuji.realtime.util.TimeType;

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
public class TimeRound extends BaseFunction {

	private static final long serialVersionUID = 1L;

	private TimeType type;
	private static Logger LOGGER = Logger.getLogger(TimeRound.class);

	public TimeRound(TimeType type) {
		this.type = type;
	}

	public void execute(TridentTuple tuple, TridentCollector collector) {
		DateTime time = new DateTime(tuple.getLong(0));
		Values values = new Values();
		switch (type) {
		case HOUR:
			int hour = time.getHourOfDay();
			values.add(hour);
			collector.emit(values);
			break;
		case DAY:
			int day = time.getDayOfWeek();
			values.add(day);
			collector.emit(values);
			break;
		case MONTH:
			int month = time.getMonthOfYear();
			values.add(month);
			collector.emit(values);
			break;
		}
	}

}
