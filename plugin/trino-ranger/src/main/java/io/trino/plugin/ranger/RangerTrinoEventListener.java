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
package io.trino.plugin.ranger;

import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryContext;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.QueryMetadata;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

class RangerTrinoEventListener
        implements EventListener
{
    private final Map<String, QueryCreatedEvent> activeQueries = new HashMap<>();

    public String getClientAddress(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryContext context = event != null ? event.getContext() : null;

        return context != null && context.getRemoteClientAddress().isPresent() ? context.getRemoteClientAddress().get() : null;
    }

    public String getQueryText(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryMetadata metadata = event != null ? event.getMetadata() : null;

        return metadata != null ? metadata.getQuery() : null;
    }

    public Instant getQueryTime(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);

        return event != null ? event.getCreateTime() : null;
    }

    public String getClientType(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryContext context = event != null ? event.getContext() : null;

        return context != null && context.getUserAgent().isPresent() ? context.getUserAgent().get() : null;
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        activeQueries.put(event.getMetadata().getQueryId(), event);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        QueryMetadata metadata = event.getMetadata();

        if (metadata != null) {
            activeQueries.remove(metadata.getQueryId());
        }
    }
}
