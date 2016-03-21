package com.xuwuji.twitter.kafka;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

import org.json.simple.JSONObject;

import com.xuwuji.stock.realtime.model.Tweet;
import com.xuwuji.stock.realtime.util.Constants;

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
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("request.required.ack", 1);
		props.put("zk.connect", Constants.ZKHOST);

	}

	public void produce(String keyword) {
		TwitterStream stream = new TwitterStreamFactory().getInstance();
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);
		StatusListener listener = new StatusListener() {
			@Override
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

			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
				System.out.println("Got a status deletion notice id:" + statusDeletionNotice.getStatusId());
			}

			@Override
			public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
				System.out.println("Got track limitation notice:" + numberOfLimitedStatuses);
			}

			@Override
			public void onScrubGeo(long userId, long upToStatusId) {
				System.out.println("Got scrub_geo event userId:" + userId + " upToStatusId:" + upToStatusId);
			}

			@Override
			public void onStallWarning(StallWarning warning) {
				System.out.println("Got stall warning:" + warning);
			}

			@Override
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
