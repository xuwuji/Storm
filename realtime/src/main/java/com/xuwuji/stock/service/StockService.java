package com.xuwuji.stock.service;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.xuwuji.realtime.util.Constants;
import com.xuwuji.realtime.util.HttpUtil;
import com.xuwuji.stock.model.Stock;

public class StockService implements Service {
	private static URLConnection connection;
	private static URL realUrl;

	static {
		try {
			realUrl = new URL(Constants.APIURL);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * get stock message
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public String run() throws IOException, ParseException {
		connection = realUrl.openConnection();
		connection = HttpUtil.setRequestHeader(connection);
		connection.connect();
		String response = HttpUtil.outputResponse(connection);
		JSONParser parser = new JSONParser();
		JSONObject obj = (JSONObject) parser.parse(response);
		JSONObject data = (JSONObject) obj.get("data");
		JSONObject market = (JSONObject) data.get("market");
		JSONObject shanghai = (JSONObject) market.get("shanghai");
		JSONObject shenzhen = (JSONObject) market.get("shenzhen");
		JSONObject DJI = (JSONObject) market.get("DJI");
		JSONObject HSI = (JSONObject) market.get("HSI");
		String shanghai_dot = shanghai.get("curdot").toString();
		String shenzhen_dot = shenzhen.get("curdot").toString();
		String DJI_dot = DJI.get("curdot").toString();
		String HSI_dot = HSI.get("curdot").toString();
		HashMap<String, Object> result = new HashMap<String, Object>();
		result.put(Stock.TIMESTAMP, System.currentTimeMillis());
		result.put(Stock.SHANGHAI, shanghai_dot);
		result.put(Stock.SHENZHEN, shenzhen_dot);
		result.put(Stock.DJI, DJI_dot);
		result.put(Stock.HSI, HSI_dot);
		return JSONValue.toJSONString(result);
	}
}
