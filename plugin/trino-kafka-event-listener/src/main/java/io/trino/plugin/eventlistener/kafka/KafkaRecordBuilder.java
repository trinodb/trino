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

import com.fasterxml.jackson.annotation.JsonFilter;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.ser.FilterProvider;
import com.fasterxml.jackson.databind.ser.impl.SimpleFilterProvider;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.eventlistener.kafka.metadata.MetadataProvider;
import io.trino.plugin.eventlistener.kafka.model.QueryCompletedEventWrapper;
import io.trino.plugin.eventlistener.kafka.model.QueryCreatedEventWrapper;
import io.trino.plugin.eventlistener.kafka.model.SplitCompletedEventWrapper;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Set;

import static com.fasterxml.jackson.databind.ser.impl.SimpleBeanPropertyFilter.serializeAllExcept;

public class KafkaRecordBuilder
{
    private final ObjectWriter writer;
    private final String startedTopic;
    private final String completedTopic;
    private final String splitCompletedTopic;
    private final MetadataProvider metadataProvider;

    @JsonFilter("property-name-filter")
    static class PropertyFilterMixIn {}

    public KafkaRecordBuilder(String startedTopic, String completedTopic, String splitCompletedTopic, Set<String> excludedFields, MetadataProvider metadataProvider)
    {
        this.startedTopic = startedTopic;
        this.completedTopic = completedTopic;
        this.splitCompletedTopic = splitCompletedTopic;
        FilterProvider filter = new SimpleFilterProvider().addFilter("property-name-filter", serializeAllExcept(excludedFields));
        this.writer = new ObjectMapperProvider().get()
                .addMixIn(Object.class, PropertyFilterMixIn.class)
                .writer(filter);
        this.metadataProvider = metadataProvider;
    }

    public ProducerRecord<String, String> buildStartedRecord(QueryCreatedEvent event)
    {
        QueryCreatedEventWrapper queryCreatedEvent = new QueryCreatedEventWrapper(event, metadataProvider.getMetadata());
        return new ProducerRecord<>(startedTopic, writeJson(queryCreatedEvent));
    }

    public ProducerRecord<String, String> buildCompletedRecord(QueryCompletedEvent event)
    {
        QueryCompletedEventWrapper queryCompletedEvent = new QueryCompletedEventWrapper(event, metadataProvider.getMetadata());
        return new ProducerRecord<>(completedTopic, writeJson(queryCompletedEvent));
    }

    public ProducerRecord<String, String> buildSplitCompletedRecord(SplitCompletedEvent event)
    {
        SplitCompletedEventWrapper splitCompletedEvent = new SplitCompletedEventWrapper(event, metadataProvider.getMetadata());
        return new ProducerRecord<>(splitCompletedTopic, writeJson(splitCompletedEvent));
    }

    private String writeJson(Object payload)
    {
        try {
            return writer.writeValueAsString(payload);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
