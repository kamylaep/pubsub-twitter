package com.kep.pubsub.subscriber;

import java.util.function.BiConsumer;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubEventSubscriber implements MessageReceiver {

  private Logger logger = LoggerFactory.getLogger(PubSubEventSubscriber.class);
  private BiConsumer<PubsubMessage, AckReplyConsumer> consumer;

  public PubSubEventSubscriber(String projectId, String subscriptionId, BiConsumer<PubsubMessage, AckReplyConsumer> consumer) {
    this.consumer = consumer;
    buildSubscriber(StringUtils.trim(projectId), StringUtils.trim(subscriptionId));
  }

  @Override
  public void receiveMessage(PubsubMessage message, AckReplyConsumer ackReplyConsumer) {
    consumer.accept(message, ackReplyConsumer);
  }

  private void buildSubscriber(String projectId, String subscriptionId) {
    ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);
    try {
      Subscriber subscriber = Subscriber.newBuilder(subscriptionName, this).build();
      subscriber.startAsync().awaitRunning();
      subscriber.awaitTerminated();
    } catch (IllegalStateException e) {
      logger.error(e.getMessage(), e);
    }
  }

}
