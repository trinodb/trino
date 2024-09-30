/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.trino.plugin.eventlistener.kafka;

import com.google.common.base.Supplier;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.log.Logger;
import io.trino.plugin.eventlistener.kafka.metadata.EnvMetadataProvider;
import io.trino.plugin.eventlistener.kafka.metadata.MetadataProvider;
import io.trino.plugin.eventlistener.kafka.metadata.NoOpMetadataProvider;
import io.trino.plugin.eventlistener.kafka.metrics.KafkaEventListenerJmxStats;
import io.trino.plugin.eventlistener.kafka.producer.KafkaProducerFactory;
import io.trino.plugin.eventlistener.kafka.producer.SSLKafkaProducerFactory;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.time.temporal.ChronoUnit.SECONDS;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class KafkaEventPublisher
{
    private static final Logger LOG = Logger.get(KafkaEventPublisher.class);

    private final KafkaRecordBuilder kafkaRecordBuilder;
    private final KafkaEventListenerJmxStats stats;
    private final Supplier<KafkaProducer<String, String>> kafkaProducerSupplier;
    @GuardedBy("this")
    private KafkaProducer<String, String> kafkaProducer;
    @GuardedBy("this")
    private RuntimeException lastKafkaProducerFailure = new RuntimeException();
    private final ScheduledExecutorService connectExecutor = Executors.newSingleThreadScheduledExecutor(daemonThreadsNamed("kafka-connect-%s"));

    public KafkaEventPublisher(KafkaEventListenerConfig config, KafkaProducerFactory producerFactory, KafkaEventListenerJmxStats stats)
    {
        this.stats = requireNonNull(stats, "stats cannot be null");
        requireNonNull(config, "config cannot be null");
        requireNonNull(producerFactory, "producerFactory cannot be null");
        checkArgument(config.getCreatedTopicName().isPresent() || config.getCompletedTopicName().isPresent(), "Either created or completed topic must be present");

        String createdTopic = config.getCreatedTopicName().orElse("");
        String completedTopic = config.getCompletedTopicName().orElse("");
        String splitCompletedTopic = config.getSplitCompletedTopicName().orElse("");
        kafkaProducerSupplier = () -> {
            Map<String, String> configOverrides = config.getKafkaClientOverrides();
            LOG.info("Creating Kafka publisher (SSL=%s) for topics: %s/%s with excluded fields: %s and kafka config overrides: %s",
                    producerFactory instanceof SSLKafkaProducerFactory, createdTopic, completedTopic, config.getExcludedFields(), configOverrides);
            KafkaProducer<String, String> kafkaProducer = producerFactory.producer(configOverrides);
            try {
                checkConnectivityToBrokers(config.getPublishCreatedEvent() ? createdTopic : completedTopic, config.getRequestTimeout().toMillis());
            }
            catch (Throwable e) {
                try {
                    kafkaProducer.close();
                }
                catch (Throwable closeException) {
                    e.addSuppressed(closeException);
                }
                LOG.error(e, "Failed to initialize Kafka publisher.");
                stats.kafkaPublisherFailedToInitialize();
                throw new RuntimeException("Cannot connect to kafka broker", e);
            }
            return kafkaProducer;
        };

        try {
            kafkaProducer = kafkaProducerSupplier.get();
        }
        catch (Exception e) {
            if (config.getTerminateOnInitializationFailure()) {
                throw e;
            }
        }

        // schedule reconnecting
        if (kafkaProducer == null) {
            connectExecutor.schedule(new Runnable()
            {
                @Override
                public void run()
                {
                    while (true) {
                        try {
                            // success
                            KafkaProducer<String, String> kafkaProducer = kafkaProducerSupplier.get();
                            synchronized (KafkaEventPublisher.this) {
                                KafkaEventPublisher.this.kafkaProducer = kafkaProducer;
                            }
                            return;
                        }
                        catch (RuntimeException e) {
                            LOG.error("Could not create Kafka producer", e);
                            synchronized (KafkaEventPublisher.this) {
                                KafkaEventPublisher.this.lastKafkaProducerFailure = e;
                            }
                            // reschedule
                            connectExecutor.schedule(this, KAFKA_CONNECT_INTERVAL.toMillis(), MILLISECONDS);
                        }
                    }
                }
            }, KAFKA_CONNECT_INTERVAL.toMillis(), MILLISECONDS);
        }

        kafkaRecordBuilder = new KafkaRecordBuilder(createdTopic, completedTopic, splitCompletedTopic, config.getExcludedFields(), metadataProvider(config));
        LOG.info("Successfully created Kafka publisher.");
    }

    private static final java.time.Duration KAFKA_CONNECT_INTERVAL = Duration.of(10, SECONDS);

    private synchronized Producer<String, String> getProducer()
    {
        if (kafkaProducer != null) {
            return kafkaProducer;
        }
        throw new RuntimeException("Could not initialize Kafka producer; waiting for retry", lastKafkaProducerFailure);
    }

    private void checkConnectivityToBrokers(String topic, long requestTimeout)
            throws Exception
    {
        LOG.info("checking connectivity to brokers (fetching partitions for topic=%s).", topic);
        CompletableFuture<Void> future = CompletableFuture.runAsync(() -> getProducer().partitionsFor(topic));
        future.get(requestTimeout, MILLISECONDS);
        LOG.info("connectivity check succeeded.");
    }

    private MetadataProvider metadataProvider(KafkaEventListenerConfig config)
    {
        if (config.getEnvironmentVariablePrefix().isPresent()) {
            return new EnvMetadataProvider(config.getEnvironmentVariablePrefix().get());
        }
        return new NoOpMetadataProvider();
    }

    public void publishCompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        stats.completedEventReceived();
        String queryId = queryCompletedEvent.getMetadata().getQueryId();
        LOG.debug("preparing to send QueryCompletedEvent for query id: %s", queryId);
        ProducerRecord<String, String> record = null;
        try {
            record = kafkaRecordBuilder.buildCompletedRecord(queryCompletedEvent);
        }
        catch (Exception e) {
            stats.completedEventBuildFailure();
            LOG.warn(e, "unable to build QueryCompletedEvent for query id: %s", queryId);
        }
        if (record != null) {
            getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    switch (exception) {
                        case TimeoutException e -> stats.completedEventSendFailureTimeout();
                        case RecordTooLargeException e -> stats.completedEventSendFailureTooLarge();
                        case InvalidRecordException e -> stats.completedEventSendFailureInvalidRecord();
                        default -> stats.completedEventSendFailureOther();
                    }
                    LOG.warn(exception, "failed to send QueryCompletedEvent for query id: %s. Uncompressed message size: %s. Partition: %s",
                            queryId, metadata.serializedValueSize(), metadata.partition());
                }
                else {
                    stats.completedEventSuccessfulDispatch();
                    LOG.debug("successfully sent QueryCompletedEvent for query id: %s", queryId);
                }
            });
        }
    }

    public void publishCreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
        stats.createdEventReceived();
        String queryId = queryCreatedEvent.getMetadata().getQueryId();
        LOG.debug("preparing to send QueryCreatedEvent for query id: %s", queryId);
        ProducerRecord<String, String> record = null;
        try {
            record = kafkaRecordBuilder.buildStartedRecord(queryCreatedEvent);
        }
        catch (Exception e) {
            stats.createdEventBuildFailure();
            LOG.warn(e, "unable to build QueryCreatedEvent for query id: %s", queryId);
        }
        if (record != null) {
            getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    switch (exception) {
                        case TimeoutException e -> stats.createdEventSendFailureTimeout();
                        case RecordTooLargeException e -> stats.createdEventSendFailureTooLarge();
                        case InvalidRecordException e -> stats.createdEventSendFailureInvalidRecord();
                        default -> stats.createdEventSendFailureOther();
                    }
                    LOG.warn(exception, "failed to send QueryCreatedEvent for query id: %s. Uncompressed message size: %s. Partition: %s",
                            queryId, metadata.serializedValueSize(), metadata.partition());
                }
                else {
                    stats.createdEventSuccessfulDispatch();
                    LOG.debug("successfully sent QueryCreatedEvent for query id: %s", queryId);
                }
            });
        }
    }

    public void publishSplitCompletedEvent(SplitCompletedEvent splitCompletedEvent)
    {
        stats.splitCompletedEventReceived();
        String queryId = splitCompletedEvent.getQueryId();
        LOG.debug("preparing to send SplitCompletedEvent for query id: %s", queryId);
        ProducerRecord<String, String> record = null;
        try {
            record = kafkaRecordBuilder.buildSplitCompletedRecord(splitCompletedEvent);
        }
        catch (Exception e) {
            stats.splitCompletedEventBuildFailure();
            LOG.warn(e, "unable to build SplitCompletedEvent for query id: %s", queryId);
        }
        if (record != null) {
            getProducer().send(record, (metadata, exception) -> {
                if (exception != null) {
                    switch (exception) {
                        case TimeoutException e -> stats.splitCompletedEventSendFailureTimeout();
                        case RecordTooLargeException e -> stats.splitCompletedEventSendFailureTooLarge();
                        case InvalidRecordException e -> stats.splitCompletedEventSendFailureInvalidRecord();
                        default -> stats.splitCompletedEventSendFailureOther();
                    }
                    LOG.warn(exception, "failed to send SplitCompletedEvent for query id: %s. Uncompressed message size: %s. Partition: %s",
                            queryId, metadata.serializedValueSize(), metadata.partition());
                }
                else {
                    stats.splitCompletedEventSuccessfulDispatch();
                    LOG.debug("successfully sent SplitCompletedEvent for query id: %s", queryId);
                }
            });
        }
    }

    public void shutdown()
    {
        getProducer().close();
    }
}
