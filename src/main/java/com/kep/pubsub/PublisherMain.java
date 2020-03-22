package com.kep.pubsub;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.kep.pubsub.publisher.PubSubEventPublisher;
import com.kep.pubsub.twitter.TwitterClient;

public class PublisherMain {

  private String projectId;
  private String tweetTopicId;
  private String userTopicId;
  private String[] twitterTerms;

  private PubSubEventPublisher tweetPublisher;
  private PubSubEventPublisher userPublisher;
  private TwitterClient twitterClient;

  private Gson gson = new Gson();

  public static void main(String[] args) {
    new PublisherMain().startPublishing();
  }

  public PublisherMain() {
    projectId = System.getProperty("PROJECT_ID");
    tweetTopicId = System.getProperty("TWEET_TOPIC_ID");
    userTopicId = System.getProperty("USER_TOPIC_ID");
    twitterTerms = getTwitterTerms();

    tweetPublisher = new PubSubEventPublisher(projectId, tweetTopicId);
    userPublisher = new PubSubEventPublisher(projectId, userTopicId);
    twitterClient = new TwitterClient(twitterTerms);
  }

  private void startPublishing() {
    twitterClient.getTweets(tweetJson -> {
      JsonObject tweet = gson.fromJson(tweetJson, JsonObject.class);

      Map<String, String> userData = parseUserData(tweet);
      userPublisher.send(gson.toJson(userData));

      Map<String, String> tweetData = parseTweetData(tweet, userData);
      tweetPublisher.send(gson.toJson(tweetData));
    });
  }

  private Map<String, String> parseUserData(JsonObject tweet) {
    JsonObject user = tweet.get("user").getAsJsonObject();
    String userId = user.get("id_str").getAsString();

    Map<String, String> userData = new HashMap<>();
    userData.put("user.id", userId);
    userData.put("user.name", user.get("name").getAsString());
    userData.put("user.screen_name", user.get("screen_name").getAsString());
    userData.put("user.friends_count", user.get("friends_count").getAsString());
    userData.put("user.followers_count", user.get("followers_count").getAsString());
    return userData;
  }

  private Map<String, String> parseTweetData(JsonObject tweet, Map<String, String> userData) {
    Map<String, String> tweetData = new HashMap<>();
    tweetData.put("user.id", userData.get("user.id"));
    tweetData.put("id", tweet.get("id_str").getAsString());
    tweetData.put("text", tweet.get("text").getAsString());
    tweetData.put("source", tweet.get("source").getAsString());
    return tweetData;
  }

  private String[] getTwitterTerms() {
    String[] twitterTerms = StringUtils.split(System.getProperty("TWITTER_TERMS"), ",");
    return Arrays.stream(twitterTerms).map(StringUtils::trim).toArray(String[]::new);
  }
}
