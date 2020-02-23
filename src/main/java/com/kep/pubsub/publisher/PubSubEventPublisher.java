package com.kep.pubsub.publisher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutures;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;

public class PubSubEventPublisher {

  private Logger logger = LoggerFactory.getLogger(PubSubEventPublisher.class);

  private Publisher publisher;
  private List<ApiFuture<String>> futures;

  public PubSubEventPublisher(String projectId, String topicId) {
    publisher = buildPublisher(StringUtils.trim(projectId), StringUtils.trim(topicId));
    futures = new ArrayList<>();
    addShutdownHook();
  }

  public void send(String value) {
    ByteString data = ByteString.copyFromUtf8(value);
    PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();
    futures.add(publisher.publish(pubsubMessage));
  }

  private Publisher buildPublisher(String projectId, String topicId) {
    ProjectTopicName topicName = ProjectTopicName.of(projectId, topicId);
    try {
      return Publisher.newBuilder(topicName).build();
    } catch (IOException e) {
      logger.error(e.getMessage(), e);
      throw new RuntimeException(e);
    }
  }

  private void addShutdownHook() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      logger.info("Shutting down publisher ");

      try {
        logger.info("Finishing publish messages");
        ApiFutures.allAsList(futures).get();
      } catch (Exception e) {
        logger.error(e.getMessage(), e);
      }

      publisher.shutdown();
      logger.info("Done shutting down publisher");
    }));
  }

}
