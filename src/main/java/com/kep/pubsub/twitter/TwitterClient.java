package com.kep.pubsub.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public class TwitterClient {

  public String apiKey = System.getProperty("API_KEY");
  public String apiSecretKey = System.getProperty("API_SECRET_KEY");
  public String accessToken = System.getProperty("ACCESS_TOKEN");
  public String accessTokenSecret = System.getProperty("ACCESS_TOKEN_SECRET");

  private Logger logger = LoggerFactory.getLogger(TwitterClient.class);
  private BlockingQueue<String> msgQueue;
  private Client twitterClient;

  public TwitterClient(String... terms) {
    logger.debug("Creating TwitterClient with terms={}", Arrays.toString(terms));
    msgQueue = new LinkedBlockingQueue<>(100000);
    twitterClient = buildTwitterClient(msgQueue, Arrays.asList(terms));
    addShutdownHook();
  }

  public void getTweets(Consumer<String> callback) {
    twitterClient.connect();
    try {
      while (!twitterClient.isDone()) {
        String tweet = msgQueue.take();
        logger.trace("Processing twee={}", tweet);
        callback.accept(tweet);
      }
    } catch (InterruptedException e) {
      logger.error("Error fetching tweets", e);
    } finally {
      twitterClient.stop();
    }
  }

  private Client buildTwitterClient(BlockingQueue<String> msgQueue, List<String> terms) {
    Hosts hbHosts = new HttpHosts(Constants.STREAM_HOST);
    StatusesFilterEndpoint hbEndpoint = new StatusesFilterEndpoint();

    hbEndpoint.trackTerms(terms);

    Authentication hbAuth = new OAuth1(apiKey, apiSecretKey, accessToken, accessTokenSecret);

    ClientBuilder builder = new ClientBuilder()
        .name("HB-Client-01")
        .hosts(hbHosts)
        .authentication(hbAuth)
        .endpoint(hbEndpoint)
        .processor(new StringDelimitedProcessor(msgQueue));

    return builder.build();
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down twitter client");
      twitterClient.stop();
      logger.info("Done shut down twitter client");
    }));
  }

}
