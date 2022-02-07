package com.example.pubsub;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;
import org.springframework.util.concurrent.ListenableFuture;

@Service
public class PubSubSender {
    private final String topicName;
    private final PubSubPublisherTemplate publisherTemplate;

    public PubSubSender(
            @Value("${app.topic-name}") String topicName, PubSubPublisherTemplate publisherTemplate) {
        this.publisherTemplate = publisherTemplate;

        Assert.hasText(topicName, "topicName cannot be null");
        this.topicName = topicName;
    }

    public ListenableFuture<String> send(String message) {
        return publisherTemplate.publish(topicName, message);
    }
}