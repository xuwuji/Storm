package com.xuwuji.twitter.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONObject;

import com.xuwuji.realtime.util.Constants;
import com.xuwuji.stock.model.Tweet;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import twitter4j.FilterQuery;
import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

/**
 * Real-time twitter producer
 * 
 * @author wuxu 2016-3-20
 *
 */
public class TwitterProducer {
	private static Properties props;
	private Producer<String, String> producer;

	static {
		props = new Properties();
		props.put("metadata.broker.list", Constants.KAKFA_BROKER);
		/**
		 * This is used to declare the serializer class, which is used to
		 * serialize the message and store it in a proper format to be retrieved
		 * later. The default encoder takes a byte array and returns the same
		 * byte array.
		 */
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		/**
		 * Based on this setting, the producer determines when to consider the
		 * produced message as complete. You also need to set how many brokers
		 * would commit to their logs before you acknowledge the message. If the
		 * value is set to 0, it means that the producer will send the message
		 * in the fire and forget mode. If the value is set to 1, it means the
		 * producer will wait till the leader replica receives the message. If
		 * the value is set to -1, the producer will wait till all the in-sync
		 * replicas receive the message. This is definitely
		 */
		props.put("request.required.ack", 1);
		props.put("zk.connect", Constants.ZKHOST);
	}

	public void produce(String... keyword) {
		TwitterStream stream = new TwitterStreamFactory().getInstance();
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		StatusListener listener = new StatusListener() {
			public void onStatus(Status status) {
				HashMap<String, Object> result = new HashMap<String, Object>();
				System.out.println("--------------------");
				System.out.println("user name: " + status.getUser().getName());
				HashtagEntity[] entities = status.getHashtagEntities();
				List<String> tags = new ArrayList<String>();
				if (entities.length != 0) {
					System.out.print("tags:");
					for (HashtagEntity tag : entities) {
						System.out.print("#" + tag.getText() + "   ");
						tags.add(tag.getText());
					}
					System.out.println("");
				}
				System.out.println("text: " + status.getText());
				System.out.println("time: " + status.getCreatedAt());
				result.put(Tweet.USERNAME, status.getUser().getName());
				System.out.println("location: " + status.getUser().getLocation());
				result.put(Tweet.LOCATION, status.getUser().getLocation());
				System.out.println("--------------------");
				System.out.println("\n\n");
				result.put(Tweet.TAGS, tags);
				result.put(Tweet.TEXT, status.getText());
				result.put(Tweet.TIME, status.getCreatedAt().getTime());
				KeyedMessage<String, String> data = new KeyedMessage<String, String>(Constants.TWITTER_TOPIC,
						JSONObject.toJSONString(result));
				producer.send(data);
			}

			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			public void onException(Exception ex) {
				ex.printStackTrace();
			}
		};
		stream.addListener(listener);
		FilterQuery query = new FilterQuery().track(keyword);
		stream.filter(query);
	}

	public static void main(String[] args) {
		TwitterProducer producer = new TwitterProducer();
		producer.produce("NBA");
	}

}
