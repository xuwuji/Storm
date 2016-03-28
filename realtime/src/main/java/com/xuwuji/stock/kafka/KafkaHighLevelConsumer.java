package com.xuwuji.stock.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * single consumer
 * 
 * @author wuxu
 *
 */
public class KafkaHighLevelConsumer {

	private static Properties props = new Properties();;
	private ConsumerConnector kafkaConsumer;

	static {
		// 1.1 zookeeper
		props.put("zookeeper.connect", "localhost:2181");
		// 1.2 the name for the consumer group shared by all the consumers
		// within the group
		props.put("group.id", "stockGroup");
		props.put("zookeeper.session.timeout.ms", "5000");
		props.put("zookeeper.sync.time.ms", "250");
		props.put("auto.commit.interval.ms", "1000");
	}

	public KafkaHighLevelConsumer() {
		ConsumerConfig config = new ConsumerConfig(props);
		kafkaConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	public void consume(String topic) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		// 1 define how many threads for the topic
		map.put(topic, new Integer(1));
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamMap = kafkaConsumer.createMessageStreams(map);
		List<KafkaStream<byte[], byte[]>> streamList = consumerStreamMap.get(topic);
		for (final KafkaStream<byte[], byte[]> stream : streamList) {
			ConsumerIterator<byte[], byte[]> iterator = stream.iterator();
			while (iterator.hasNext()) {
				System.out.println("message:" + new String(iterator.next().message()));
			}
		}
		if (kafkaConsumer != null) {
			kafkaConsumer.shutdown();
		}
	}

	public static void main(String[] args) {
		KafkaHighLevelConsumer k = new KafkaHighLevelConsumer();
		k.consume("stock.dot");
	}

}
