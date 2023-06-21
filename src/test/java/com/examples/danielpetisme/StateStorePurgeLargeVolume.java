package com.examples.danielpetisme;

import io.github.netmikey.logunit.api.LogCapturer;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

import static org.apache.kafka.clients.admin.AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class StateStorePurgeLargeVolume {

    public static final int NB_PARTITIONS = 1;
    public static final long MESSAGES_PER_PARTITIONS = 1_000_000L;

    public static final long LIMIT = 800_000L;
    public static final long PURGE_INTERVAL_MS = 10_000L;

    public static final String PURGE_STATE_STORE_NAME = "purgeStateStore";

    private final static Logger LOGGER = LoggerFactory.getLogger(StateStorePurgeLargeVolume.class);

    public static final String BASE_CLIENT_ID = StateStorePurgeLargeVolume.class.getName();
    static final String INPUT_TOPIC = "in";

    static final String OUTPUT_TOPIC = "out";

    @Container
    public static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka"));
    public KafkaStreams streams;

    @BeforeEach
    public void beforeEach() throws Exception {
        createTopics(List.of(INPUT_TOPIC, OUTPUT_TOPIC), NB_PARTITIONS, 1);

        Properties producerProperties = new Properties();
        producerProperties.putAll(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                ProducerConfig.CLIENT_ID_CONFIG, String.format("%s-producer", BASE_CLIENT_ID)
        ));

        try (final KafkaProducer<String, Long> producer = new KafkaProducer<>(producerProperties, new StringSerializer(), new LongSerializer())) {
            long startSending = System.currentTimeMillis();
            for (long i = 0; i < NB_PARTITIONS * MESSAGES_PER_PARTITIONS; i++) {
                String keyPrefix = i % 1000 == 0 ? "EOM_" : "EOD_";
                ProducerRecord<String, Long> record = new ProducerRecord<>(INPUT_TOPIC, keyPrefix + UUID.randomUUID(), i);
                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        LOGGER.error("Error sending record: {}", exception.getMessage());
                    } else {
                        LOGGER.debug("Record sent: Topic: {}, Partition: {}, Offset: {}, Key:{}, Value:{}", metadata.topic(), metadata.partition(), metadata.offset(), record.key(), record.value());
                    }
                });
            }
            long timeToSent = System.currentTimeMillis() - startSending;
            LOGGER.info("Time to send {} messages: {} ms", NB_PARTITIONS * MESSAGES_PER_PARTITIONS, timeToSent);

        }
    }

    @AfterEach
    public void afterEach() {
        streams.close(Duration.ofSeconds(3));
    }

    @Test
    public void test() throws Exception {

        Properties streamProperties = new Properties();
        streamProperties.putAll(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                StreamsConfig.APPLICATION_ID_CONFIG, String.format("%s-test-streams", BASE_CLIENT_ID),
                StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
                StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest"
        ));

        StoreBuilder<KeyValueStore<String, Long>> purgeStateStoreBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(PURGE_STATE_STORE_NAME),
                Serdes.String(),
                Serdes.Long());

        final var builder = new StreamsBuilder();
        builder.addStateStore(purgeStateStoreBuilder);

        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.String(), Serdes.Long()))
                .process(() -> new StateStorePurgeProcessor(
                        PURGE_STATE_STORE_NAME,
                        PURGE_INTERVAL_MS,
                        "EOD_",
                        (it) -> it.value < LIMIT
                ), PURGE_STATE_STORE_NAME)
                .to(OUTPUT_TOPIC,
                        Produced.with(Serdes.String(), Serdes.Long()));

        streams = new KafkaStreams(builder.build(), streamProperties);
        streams.setUncaughtExceptionHandler((exception) -> {
            LOGGER.error("Uncaught exception: {}", exception.getMessage());
            return SHUTDOWN_APPLICATION;
        });
        streams.start();


        KafkaConsumer<String, Long> outputConsumer = new KafkaConsumer<>(
                Map.of(
                        BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers(),
                        ConsumerConfig.GROUP_ID_CONFIG, String.format("%s-consumer", BASE_CLIENT_ID),
                        ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
                ),
                new StringDeserializer(), new LongDeserializer()
        );

        outputConsumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    List<ConsumerRecord> loaded = new ArrayList<>();
                    ConsumerRecords<String, Long> records = outputConsumer.poll(Duration.of(10, ChronoUnit.SECONDS));
                    records.forEach((record) -> {
                        loaded.add(record);
                        System.out.println("Output Topic: " + record.value());
                    });
                    assertThat(loaded).isNotEmpty();
                    assertThat(loaded.size()).isEqualTo(1);
                });

        ReadOnlyKeyValueStore<String, Long> readOnlyPurgeStateStore =
                streams.store(StoreQueryParameters.fromNameAndType(PURGE_STATE_STORE_NAME, QueryableStoreTypes.keyValueStore()));

        assertThat(readOnlyPurgeStateStore.approximateNumEntries()).isEqualTo(200_800L);
        readOnlyPurgeStateStore.all().forEachRemaining((record) -> {
            assertThat(record).satisfiesAnyOf(
                    it -> assertThat(it.key).startsWith("EOM_"),
                    it -> assertThat(it.value).isGreaterThan(LIMIT)
            );
        });

    }


    public static class StateStorePurgeProcessor implements Processor<String, Long, String, Long> {

        private ProcessorContext context;
        private KeyValueStore<String, Long> store;
        public final String purgeStateStoreName;
        public final long purgeInternalMs;
        public final String keyPrefix;
        public final Predicate<KeyValue<String, Long>> purgeRecordPredicate;

        public StateStorePurgeProcessor(String purgeStateStoreName, long purgeInternalMs, String keyPrefix, Predicate<KeyValue<String, Long>> purgeRecordPredicate) {
            this.purgeStateStoreName = purgeStateStoreName;
            this.purgeInternalMs = purgeInternalMs;
            this.keyPrefix = keyPrefix;
            this.purgeRecordPredicate = purgeRecordPredicate;
        }

        @Override
        public void init(ProcessorContext context) {
            this.context = context;
            store = this.context.getStateStore(purgeStateStoreName);

            this.context.schedule(Duration.ofMillis(purgeInternalMs), PunctuationType.WALL_CLOCK_TIME, (timestamp) -> {
                LOGGER.info("Approximate number of entries before purge: {}", store.approximateNumEntries());
                long startPurging = System.currentTimeMillis();
                try (final KeyValueIterator<String, Long> all = store.prefixScan(this.keyPrefix, new StringSerializer())) {
                    while (all.hasNext()) {
                        final KeyValue<String, Long> next = all.next();
                        if (purgeRecordPredicate.test(next)) {
                            store.delete(next.key);
                        }
                    }
                    context.forward(new Record("done", -1L, System.currentTimeMillis()));
                }
                long timeToPurge = System.currentTimeMillis() - startPurging;
                LOGGER.info("Time to purge: {} ms", timeToPurge);
                LOGGER.info("Approximate number of entries after purge: {}", store.approximateNumEntries());
            });
        }

        @Override
        public void process(Record<String, Long> record) {
            store.put(record.key(), record.value());
        }

        @Override
        public void close() {

        }
    }

    private static void createTopics(List<String> topics, int partitions, int rf) throws InterruptedException, ExecutionException {
        var adminClient = AdminClient.create(Map.of(
                BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers()
        ));

        for (String topicName : topics) {
            if (!adminClient.listTopics().names().get().contains(topicName)) {
                LOGGER.info("Creating topic {}", topicName);
                final NewTopic newTopic = new NewTopic(topicName, partitions, (short) rf);
                try {
                    CreateTopicsResult topicsCreationResult = adminClient.createTopics(Collections.singleton(newTopic));
                    topicsCreationResult.all().get();
                } catch (Exception e) {
                    //silent ignore if topic already exists
                }
            }
        }
    }

}
