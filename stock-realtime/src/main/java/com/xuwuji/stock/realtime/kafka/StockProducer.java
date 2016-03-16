package com.xuwuji.stock.realtime.kafka;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.HashMap;
import java.util.Properties;

import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.xuwuji.stock.realtime.model.Stock;
import com.xuwuji.stock.realtime.util.Constants;
import com.xuwuji.stock.realtime.util.HttpUtil;
import com.xuwuji.stock.realtime.util.TimeUtil;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * produce topic message
 * 
 * @author wuxu 2016-3-15
 *
 */
public class StockProducer {

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
	 * stock service api
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	public String getStock() throws IOException, ParseException {
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
		Double shanghai_dot = Double.valueOf(shanghai.get("curdot").toString());
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

	/**
	 * produce the message
	 * 
	 * @throws InterruptedException
	 * @throws ParseException
	 * @throws IOException
	 */
	public void produce() throws InterruptedException, IOException, ParseException {

		// 1.Set the properties for kafka cluster
		Properties props = new Properties();
		// 1.1 Kafka broker list
		props.put("metadata.broker.list", "localhost:9092");
		// 1.2 Serializer class
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		// 1.3 Ack
		props.put("request.required.acks", "1");
		// 1.4 ZK Server
		props.put("zk.connect", "localhost:2181");

		// 2. producer config
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		String topic = Constants.STOCK_TOPIC;
		for (int i = 0; i < 1000; i++) {
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, this.getStock());
			System.out.println(this.getStock());
			producer.send(data);
			Thread.sleep(1000);
		}
		producer.close();
	}

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		StockProducer p = new StockProducer();
		p.produce();

	}

}
