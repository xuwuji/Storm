package com.xuwuji.stock.kafka;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

/**
 * This is an implementation of a multithread Consumer
 * 
 * Usually based on the number of partitions in the topic and follows a
 * one-to-one mapping approach between the thread and the partitions within the
 * topic. FOr example, if four partitions are defined for any topic, as the best
 * practice, only four threads should be initiated with the consumer application
 * to read the data.
 * 
 * @author wuxu 2016-3-16
 *
 */
public class MultiThreadConsumer {
	private static Properties props = new Properties();;
	private ConsumerConnector kafkaConsumer;
	private ExecutorService executor;

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

	public MultiThreadConsumer() {
		ConsumerConfig config = new ConsumerConfig(props);
		kafkaConsumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
	}

	public void consume(String topic, Integer threadCount) {
		Map<String, Integer> map = new HashMap<String, Integer>();
		map.put(topic, threadCount);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerStreamMap = kafkaConsumer.createMessageStreams(map);
		List<KafkaStream<byte[], byte[]>> streams = consumerStreamMap.get(topic);
		executor = Executors.newFixedThreadPool(threadCount);
		for (final KafkaStream<byte[], byte[]> stream : streams) {
			executor.submit(new Runnable() {
				ConsumerIterator<byte[], byte[]> iterator = stream.iterator();

				public void run() {
					while (iterator.hasNext()) {
						System.out.println("message:" + new String(iterator.next().message()));
					}
				}
			});
		}
		if (kafkaConsumer != null) {
			kafkaConsumer.shutdown();
		}
		if (executor != null) {
			executor.shutdown();
		}
	}
}
