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
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

class RangerTrinoEventListener
        implements EventListener
{
    private final Map<String, QueryCreatedEvent> activeQueries = new ConcurrentHashMap<>();

    public Optional<String> getClientAddress(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryContext context = event != null ? event.getContext() : null;

        return context != null && context.getRemoteClientAddress().isPresent() ? Optional.of(context.getRemoteClientAddress().get()) : Optional.empty();
    }

    public Optional<String> getQueryText(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryMetadata metadata = event != null ? event.getMetadata() : null;

        return metadata != null ? Optional.of(metadata.getQuery()) : Optional.empty();
    }

    public Optional<Instant> getQueryTime(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);

        return event != null ? Optional.of(event.getCreateTime()) : Optional.empty();
    }

    public Optional<String> getClientType(String queryId)
    {
        QueryCreatedEvent event = activeQueries.get(queryId);
        QueryContext context = event != null ? event.getContext() : null;

        return context != null && context.getUserAgent().isPresent() ? Optional.of(context.getUserAgent().get()) : Optional.empty();
    }

    @Override
    public void queryCreated(QueryCreatedEvent event)
    {
        activeQueries.put(event.getMetadata().getQueryId(), event);
    }

    @Override
    public void queryCompleted(QueryCompletedEvent event)
    {
        activeQueries.remove(event.getMetadata().getQueryId());
    }
}
