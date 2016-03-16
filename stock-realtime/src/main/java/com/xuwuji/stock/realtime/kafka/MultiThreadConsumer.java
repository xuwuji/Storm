package com.xuwuji.stock.realtime.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutorService;

import kafka.consumer.ConsumerConfig;
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

	public void consume(String topic) {

	}
}
