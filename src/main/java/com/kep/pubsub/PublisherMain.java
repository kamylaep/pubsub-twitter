package com.kep.pubsub;

import java.util.Arrays;

import org.apache.commons.lang3.StringUtils;

import com.kep.pubsub.publisher.PubSubEventPublisher;
import com.kep.pubsub.twitter.TwitterClient;

public class PublisherMain {

  public static void main(String[] args) {
    String projectId = System.getProperty("PROJECT_ID");
    String topicId = System.getProperty("TOPIC_ID");
    String[] twitterTerms = getTwitterTerms();

    PubSubEventPublisher pubSubEventPublisher = new PubSubEventPublisher(projectId, topicId);
    TwitterClient twitterClient = new TwitterClient(twitterTerms);
    twitterClient.getTweets(tweet -> pubSubEventPublisher.send(tweet));
  }

  private static String[] getTwitterTerms() {
    String[] twitterTerms = StringUtils.split(System.getProperty("TWITTER_TERMS"), ",");
    return Arrays.stream(twitterTerms).map(StringUtils::trim).toArray(String[]::new);
  }
}
