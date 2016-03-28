package com.xuwuji.stock.storm.bolt;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class MovingAverageBolt extends BaseBasicBolt {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private double sum = 0;
	int count = 0;
	double average = 0;
	private List<Double> list = new ArrayList<Double>();
	private static final Logger LOGGER = Logger.getLogger(DotDataBoltFilter.class);

	public void execute(Tuple input, BasicOutputCollector collector) {
		// LOGGER.error(String.valueOf("--------" + input.size()));
		double dot = Double.valueOf(input.getString(0));
		// LOGGER.error(String.valueOf("--------" + dot));
		// 1. Calculate the average at this point
		count++;
		sum = sum + dot;
		average = sum / count;

		// 2. Calculate average of all data in the list
		double temp_sum = 0;
		double temp_averge = 0;
		for (Double d : list) {
			temp_sum += d;
		}
		if (list.size() != 0) {
			temp_averge = temp_sum / list.size();
		}
		if (Math.abs(average - temp_averge) > 1) {
			LOGGER.error("--------------" + dot + "is high --------------");
		} else {
			list.add(average);
		}
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
