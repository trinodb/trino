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

import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.eventlistener.kafka.metrics.KafkaEventListenerJmxStats;
import io.trino.plugin.eventlistener.kafka.producer.KafkaProducerFactory;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.Nullable;

public class KafkaEventListener
        implements EventListener
{
    private static final Logger LOG = Logger.get(KafkaEventListener.class);

    private final KafkaEventListenerJmxStats stats = new KafkaEventListenerJmxStats();
    private final boolean publishCreatedEvent;
    private final boolean publishCompletedEvent;
    private final boolean publishSplitCompletedEvent;
    private final boolean isAnonymizationEnabled;
    @Nullable
    private KafkaEventPublisher kafkaPublisher;

    @Inject
    public KafkaEventListener(KafkaEventListenerConfig config, KafkaProducerFactory producerFactory)
            throws Exception
    {
        publishCreatedEvent = config.getPublishCreatedEvent();
        publishCompletedEvent = config.getPublishCompletedEvent();
        publishSplitCompletedEvent = config.getPublishSplitCompletedEvent();
        isAnonymizationEnabled = config.isAnonymizationEnabled();

        try {
            if (publishCreatedEvent || publishCompletedEvent) {
                kafkaPublisher = new KafkaEventPublisher(config, producerFactory, stats);
            }
            else {
                LOG.warn("Event listener will be no-op, as neither created events nor completed events are published.");
            }
        }
        catch (Exception e) {
            if (config.getTerminateOnInitializationFailure()) {
                throw e;
            }
            LOG.error(e, "Failed to initialize Kafka publisher.");
            stats.kafkaPublisherFailedToInitialize();
        }
    }

    @Managed
    @Flatten
    public KafkaEventListenerJmxStats getStats()
    {
        return stats;
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        if (kafkaPublisher != null && publishCreatedEvent) {
            kafkaPublisher.publishCreatedEvent(event);
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        if (kafkaPublisher != null && publishCompletedEvent) {
            kafkaPublisher.publishCompletedEvent(event);
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent event)
    {
        if (kafkaPublisher != null && publishSplitCompletedEvent) {
            kafkaPublisher.publishSplitCompletedEvent(event);
        }
    }

    @Override
    public boolean requiresAnonymizedPlan()
    {
        return isAnonymizationEnabled;
    }

    @Override
    public void shutdown()
    {
        if (kafkaPublisher != null) {
            kafkaPublisher.shutdown();
        }
    }
}
