package com.xuwuji.twitter.storm.trident.operation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.simple.JSONValue;

import com.xuwuji.stock.model.Tweet;

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
public class TweetParser extends BaseFunction {
	private static final long serialVersionUID = 1L;
	private String[] fields;

	public TweetParser(String[] fields) {
		this.fields = fields;
	}

	@SuppressWarnings("unchecked")
	public void execute(TridentTuple tuple, TridentCollector collector) {
		String str = tuple.getString(0);
		Map<String, Object> map = new HashMap<String, Object>();
		map = (Map<String, Object>) JSONValue.parse(str);

		// null check
		for (String field : fields) {
			if (map.get(field) == null) {
				return;
			}
		}

		// get tags
		ArrayList<String> tags = new ArrayList<String>();
		List<String> list = (List<String>) map.get(Tweet.TAGS);
		for (String tag : list) {
			tags.add(tag);
		}

		// emit the values,separate one message with multiple-tags into
		// multiple-messages with one tag
		for (String tag : tags) {
			Values values = new Values();
			for (String attribute : fields) {
				if (!attribute.equals(Tweet.TAGS)) {
					values.add(map.get(attribute));
				}
			}
			values.add(tag);
			collector.emit(values);
		}
	}
}
