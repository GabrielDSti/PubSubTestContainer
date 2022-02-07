package com.example.pubsub;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import com.google.cloud.pubsub.v1.SubscriptionAdminClient;
import com.google.cloud.pubsub.v1.SubscriptionAdminSettings;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.cloud.pubsub.v1.TopicAdminSettings;
import com.google.pubsub.v1.PubsubMessage;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.gcp.pubsub.PubSubAdmin;
import org.springframework.cloud.gcp.pubsub.core.publisher.PubSubPublisherTemplate;
import org.springframework.cloud.gcp.pubsub.core.subscriber.PubSubSubscriberTemplate;
import org.springframework.cloud.gcp.pubsub.support.AcknowledgeablePubsubMessage;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.util.concurrent.ListenableFuture;
import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.awaitility.Awaitility.await;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest
@Testcontainers
@ActiveProfiles("test")
public class PubSubIntegrationTests {
    private static final String PROJECT_ID = "test-project";

    @Autowired
    PubSubSender sender;
    @Autowired
    PubSubSubscriberTemplate subscriberTemplate;
    @Autowired
    PubSubPublisherTemplate publisherTemplate;

    @Container
    public static PubSubEmulatorContainer pubsubEmulator = new PubSubEmulatorContainer(
            DockerImageName.parse("gcr.io/google.com/cloudsdktool/cloud-sdk:367.0.0-emulators")
    );

    @DynamicPropertySource
    static void emulatorProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.gcp.pubsub.emulator-host", pubsubEmulator::getEmulatorEndpoint);
    }

    @BeforeAll
    static void setup() throws Exception {
        ManagedChannel channel =
                ManagedChannelBuilder.forTarget("dns:///" + pubsubEmulator.getEmulatorEndpoint())
                        .usePlaintext()
                        .build();
        TransportChannelProvider channelProvider =
                FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));

        TopicAdminClient topicAdminClient =
                TopicAdminClient.create(
                        TopicAdminSettings.newBuilder()
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .setTransportChannelProvider(channelProvider)
                                .build());

        SubscriptionAdminClient subscriptionAdminClient =
                SubscriptionAdminClient.create(
                        SubscriptionAdminSettings.newBuilder()
                                .setTransportChannelProvider(channelProvider)
                                .setCredentialsProvider(NoCredentialsProvider.create())
                                .build());

        PubSubAdmin admin =
                new PubSubAdmin(() -> PROJECT_ID, topicAdminClient, subscriptionAdminClient);

        admin.createTopic("test-topic");
        admin.createSubscription("test-subscription", "test-topic");

        admin.close();
        channel.shutdown();
    }

    /**
     * Por padrão, a configuração automática inicializará as credenciais padrão do aplicativo.
     * Para fins de teste, não use nenhuma credencial. Bootstrap com NoCredentialsProvider.
     */
    @TestConfiguration
    static class PubSubEmulatorConfiguration {
        @Bean
        CredentialsProvider googleCredentials() {
            return NoCredentialsProvider.create();
        }
    }

    @Test
    void shouldSendMessageToSubscription() throws ExecutionException, InterruptedException {
        ListenableFuture<String> future = sender.send("This is a test made by Gabriel.");

        List<AcknowledgeablePubsubMessage> msgs =
                await().until(() -> subscriberTemplate.pull("test-subscription", 10, true), not(empty()));

        assertEquals(1, msgs.size());
        assertEquals(future.get(), msgs.get(0).getPubsubMessage().getMessageId());
        assertEquals("This is a test made by Gabriel.", msgs.get(0).getPubsubMessage().getData().toStringUtf8());

        for (AcknowledgeablePubsubMessage msg : msgs) {
            msg.ack();
        }
    }

    @Test
    void shouldPublishMessageInTopic() throws ExecutionException, InterruptedException {
        ListenableFuture<String> future = publisherTemplate.publish("test-topic", "This is a test made by Gabriel.");

        List<PubsubMessage> messages = Collections.synchronizedList(new LinkedList<>());
        PubSubWorker worker =
                new PubSubWorker(
                        "test-subscription",
                        subscriberTemplate,
                        (msg) -> {
                            messages.add(msg);
                        });
        worker.start();

        await().until(() -> messages, not(empty()));
        assertEquals(1, messages.size());
        assertEquals(future.get(), messages.get(0).getMessageId());
        assertEquals("This is a test made by Gabriel.", messages.get(0).getData().toStringUtf8());

        worker.stop();
    }

    @AfterEach
    void teardown() {
       //remove todas as mensagens que estão na assinatura, para que não interfira em possiveis testes futuros.
        await().until(() -> subscriberTemplate.pullAndAck("test-subscription", 1000, true), hasSize(0));
    }
}
