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
import com.xuwuji.stock.realtime.service.Service;
import com.xuwuji.stock.realtime.service.StockService;
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
public class KafkaProducer {

	private Service service;

	public KafkaProducer(Service service) {
		this.service = service;
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
		// 1.5 define a partition rule
		// props.put("partitioner.class",
		// "com.xuwuji.stock.realtime.kafka.CustomPartitioner");

		// 2. producer config
		ProducerConfig config = new ProducerConfig(props);
		Producer<String, String> producer = new Producer<String, String>(config);

		String topic = Constants.STOCK_TOPIC;
		for (int i = 0; i < 1000; i++) {
			String message = (String) service.run();
			KeyedMessage<String, String> data = new KeyedMessage<String, String>(topic, message);

			/**
			 * if you define a partitioner, then you need to set the key in the
			 * second parameter in order for the partitioner to do its job
			 */
			// KeyedMessage<String, String> data = new KeyedMessage<String,
			// String>(topic,{KEY}, message);

			System.out.println(message);
			producer.send(data);
			Thread.sleep(1000);
		}
		producer.close();
	}

	public static void main(String[] args) throws IOException, ParseException, InterruptedException {
		KafkaProducer p = new KafkaProducer(new StockService());
		p.produce();
	}

}
