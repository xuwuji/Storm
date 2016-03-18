package com.xuwuji.twitter;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;

public class TwitterConsumer {

	public static void main(String[] args) {
		// StatusListener listener = new TwitterStatusListener();
		TwitterStream stream = new TwitterStreamFactory().getInstance();
		StatusListener listener = new StatusListener() {
			@Override
			public void onStatus(Status status) {

				System.out.println("--------------------");
				System.out.println(status.getUser().getName());
				System.out.println(status.getUser().getLocation());
				HashtagEntity[] entities = status.getHashtagEntities();
				if (entities.length != 0) {
					for (HashtagEntity tag : entities) {
						System.out.println(tag.getText());
					}
				}

				System.out.println(status.getText());
				// System.out.println("@" + status.getUser().getScreenName() + "
				// - " + status.getText());
				System.out.println("--------------------");
				System.out.println("\n\n");
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
		stream.sample();
	}

}
