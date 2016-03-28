package com.xuwuji.realtime.util;

import java.util.HashMap;

public class Constants {
	public static HashMap<String, String> HEADER = new HashMap<String, String>();
	public static final String APIURL = "http://a.apix.cn/apixmoney/stockdata/stock?stockid=50";
	public static final String APIKEY = "39db628ef6cb48385695ee018e927d67";
	public static final String STOCK_TOPIC = "stock.dot";
	public static final String ZKHOST = "localhost:2181";
	public static final String KAKFA_BROKER = "localhost:9092";
	public static final String TWITTER_TOPIC = "twitter.message";

	static {
		HEADER.put("accept", "application/json");
		HEADER.put("content-type", "application/json");
		HEADER.put("apix-key", APIKEY);
	}

}
