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
package io.trino.plugin.httpquery;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.bootstrap.LifeCycleManager;
import io.airlift.http.server.HttpServerInfo;
import io.trino.cache.SafeCaches;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.NotFoundException;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.UriInfo;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Path("/v1/events")
public class HttpServerEventListener
        implements EventListener
{
    private final LifeCycleManager lifecycleManager;
    private final int serverPort;

    private Cache<String, QueryCompletedEvent> events;

    @Inject
    public HttpServerEventListener(
            HttpServerInfo httpServerInfo,
            LifeCycleManager lifecycleManager,
            HttpServerEventListenerConfig config)
    {
        this.serverPort = httpServerInfo.getHttpUri().getPort();
        this.lifecycleManager = requireNonNull(lifecycleManager, "lifecycleManager is null");
        requireNonNull(config, "http event listener config is null");
        events = SafeCaches.buildNonEvictableCache(CacheBuilder.newBuilder()
                .maximumSize(config.getEventBufferSize())
                .expireAfterWrite(config.getEventTTL().toMillis(), TimeUnit.MILLISECONDS));
    }

    @GET
    @Path("completedQueries/get/{queryId}")
    @Produces(MediaType.APPLICATION_JSON)
    public QueryCompletedEvent get(@PathParam("queryId") String queryId)
    {
        QueryCompletedEvent event = events.getIfPresent(queryId);
        if (event == null) {
            throw new NotFoundException("completion event for '" + queryId + "' not found");
        }
        return event;
    }

    @GET
    @Path("completedQueries/list")
    @Produces(MediaType.APPLICATION_JSON)
    public List<String> get(@Context UriInfo uriInfo)
    {
        return ImmutableList.copyOf(events.asMap().keySet());
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        // query creation event not supported
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        events.put(queryCompletedEvent.getMetadata().getQueryId(), queryCompletedEvent);
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
        // split completion events not supported
    }

    @VisibleForTesting
    int getServerPort()
    {
        return serverPort;
    }

    @Override
    public void shutdown()
    {
        lifecycleManager.stop();
    }
}
