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
package io.trino.plugin.queryeventlistener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.trino.plugin.queryeventlistener.model.CompletedEvent;
import io.trino.plugin.queryeventlistener.model.CreatedEvent;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.Map;

public class QueryEventListener
        implements EventListener
{
    private static final Logger logger = Logger.get(QueryEventListener.class);
    static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule()).disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);

    public QueryEventListener(Map<String, String> config)
    {
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        logger.info("event=%s", handleQueryCreatedEvent(queryCreatedEvent));

        if (Strings.isNullOrEmpty(queryCreatedEvent.getContext().getUser())) {
            logger.error("QueryId:'%s', Username is NULL", queryCreatedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        logger.info("event=%s", handleQueryCompletedEvent(queryCompletedEvent));

        if (Strings.isNullOrEmpty(queryCompletedEvent.getContext().getUser())) {
            logger.error("QueryId:'%s', Username is NULL", queryCompletedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }

    String handleQueryCreatedEvent(QueryCreatedEvent event)
    {
        CreatedEvent createdEvent = new CreatedEvent(event);
        try {
            return objectMapper.writeValueAsString(createdEvent);
        }
        catch (JsonProcessingException e) {
            logger.error(e, "Failed to serialize QueryCreatedEvent for queryId:'%s'", event.getMetadata().getQueryId());
            return createdEvent.toString();
        }
    }

    String handleQueryCompletedEvent(QueryCompletedEvent event)
    {
        CompletedEvent completedEvent = new CompletedEvent(event);
        try {
            return objectMapper.writeValueAsString(completedEvent);
        }
        catch (JsonProcessingException e) {
            logger.error(e, "Failed to serialize QueryCompletedEvent for queryId:'%s'", event.getMetadata().getQueryId());
            return completedEvent.toString();
        }
    }
}
