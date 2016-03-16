package com.xuwuji.stock.realtime.util;

import java.util.HashMap;

public class Constants {
	public static HashMap<String, String> HEADER = new HashMap();
	public static final String APIURL = "http://a.apix.cn/apixmoney/stockdata/stock?stockid=50";
	public static final String APIKEY = "39db628ef6cb48385695ee018e927d67";
	public static final String STOCK_TOPIC = "stock.dot";

	static {
		HEADER.put("accept", "application/json");
		HEADER.put("content-type", "application/json");
		HEADER.put("apix-key", APIKEY);
	}

}
