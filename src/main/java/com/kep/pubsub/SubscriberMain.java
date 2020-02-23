package com.kep.pubsub;

import java.util.function.BiConsumer;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.pubsub.v1.PubsubMessage;
import com.kep.pubsub.subscriber.PubSubEventSubscriber;

public class SubscriberMain {

  public static void main(String[] args) {
    String projectId = System.getProperty("PROJECT_ID");
    String subscriptionId = System.getProperty("SUBSCRIPTION_ID");

    BiConsumer<PubsubMessage, AckReplyConsumer> consumer = (message, reply) -> {
      System.out.println(message);
      reply.ack();
    };

    new PubSubEventSubscriber(projectId, subscriptionId, consumer);
  }
}
