package com.example.pubsub;

import com.google.api.core.ApiService;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.PubsubMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate;

import java.util.function.Consumer;

public class PubSubWorker {
    private static final Logger logger = LoggerFactory.getLogger(PubSubWorker.class);

    private final String subscription;
    private final PubSubSubscriberTemplate subscriberTemplate;
    private final Consumer<PubsubMessage> listener;
    private volatile Subscriber subscriber;

    public PubSubWorker(String subscription, PubSubSubscriberTemplate subscriberTemplate) {
        this.subscription=subscription;
        this.subscriberTemplate=subscriberTemplate;
        this.listener=null;
    }

    PubSubWorker(String subscription, PubSubSubscriberTemplate subscriberTemplate, Consumer<PubsubMessage> listener) {
        this.subscription = subscription;
        this.subscriberTemplate = subscriberTemplate;
        this.listener = listener;
    }

    public void start() {
        this.subscriber =
                subscriberTemplate.subscribe(
                        subscription,
                        (msg) -> {
                            msg.ack();

                            if (listener != null) {
                                listener.accept(msg.getPubsubMessage());
                            }
                        });
    }

    public void stop() {
        ApiService service = subscriber.stopAsync();
        while (service.isRunning()) {
            service.awaitTerminated();
        }
    }
}
