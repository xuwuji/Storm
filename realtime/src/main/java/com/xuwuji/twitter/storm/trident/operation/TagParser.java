package com.xuwuji.twitter.storm.trident.operation;

import java.util.List;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TagParser extends BaseFunction {

	private static final long serialVersionUID = 1L;

	@Override
	public void execute(TridentTuple tuple, TridentCollector collector) {
		// @SuppressWarnings("unchecked")
		for (int i = 0; i < tuple.size(); i++) {
			// System.out.println(tuple.get(i));
		}
		List<String> tags = (List<String>) tuple.get(0);
		// System.out.println(tags);
		if (tags.size() == 0) {
			return;
		}
		Values values = new Values();
		for (String tag : tags) {
			System.out.println("---" + tag);
			values.add(tag);
		}
		collector.emit(values);
	}

}
