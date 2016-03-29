package com.xuwuji.twitter.storm.trident.operation;

import storm.trident.operation.CombinerAggregator;
import storm.trident.tuple.TridentTuple;

/**
 * Using bulit-in Count Caused by: java.lang.ClassCastException:
 * java.lang.Integer cannot be cast to java.lang.Long at
 * storm.trident.operation.builtin.Count.combine(Count.java:24)
 * ~[storm-core-0.9.2-incubating.jar:0.9.2-incubating] at
 * storm.trident.state.CombinerValueUpdater.update(CombinerValueUpdater.java:34)
 * ~[storm-core-0.9.2-incubating.jar:0.9.2-incubating]
 */
public class Count implements CombinerAggregator<Integer> {

	@Override
	public Integer init(TridentTuple tuple) {
		return 1;
	}

	@Override
	public Integer combine(Integer val1, Integer val2) {
		return val1 + val2;
	}

	@Override
	public Integer zero() {
		return 0;
	}

}
