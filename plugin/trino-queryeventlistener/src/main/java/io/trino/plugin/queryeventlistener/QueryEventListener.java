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

import com.google.common.base.Strings;
import io.airlift.log.Logger;
import io.trino.spi.eventlistener.EventListener;
import io.trino.spi.eventlistener.QueryCompletedEvent;
import io.trino.spi.eventlistener.QueryCreatedEvent;
import io.trino.spi.eventlistener.SplitCompletedEvent;

import java.util.Map;

public class QueryEventListener
        implements EventListener
{
    private static final Logger logger = Logger.get(QueryEventListener.class);

    public QueryEventListener(Map<String, String> config)
    {
    }

    @Override
    public void queryCreated(QueryCreatedEvent queryCreatedEvent)
    {
        logger.info("EventType:'QueryCreate', QueryId:'%s', CreateTime:'%s', User:'%s', Schema:'%s', Catalog:'%s', SQLQuery:'%s'",
                queryCreatedEvent.getMetadata().getQueryId(),
                queryCreatedEvent.getCreateTime().toString(),
                queryCreatedEvent.getContext().getUser(),
                queryCreatedEvent.getContext().getSchema().orElse(""),
                queryCreatedEvent.getContext().getCatalog().orElse(""),
                queryCreatedEvent.getMetadata().getQuery());
        if (Strings.isNullOrEmpty(queryCreatedEvent.getContext().getUser())) {
            logger.error("QueryId:'%s', Error:'Username is NULL'", queryCreatedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void queryCompleted(QueryCompletedEvent queryCompletedEvent)
    {
        logger.info("EventType:'QueryComplete', QueryId:'%s', CreateTime:'%s', QueuedTime:'%s', WallTime:'%s', CpuTime:'%s', User:'%s', Schema:'%s', Catalog:'%s', Records:'%s', Completed:'%s', SQLQuery:'%s'",
                queryCompletedEvent.getMetadata().getQueryId(),
                queryCompletedEvent.getCreateTime().toString(),
                queryCompletedEvent.getStatistics().getQueuedTime(),
                queryCompletedEvent.getStatistics().getWallTime(),
                queryCompletedEvent.getStatistics().getCpuTime(),
                queryCompletedEvent.getContext().getUser(),
                queryCompletedEvent.getContext().getSchema().orElse(""),
                queryCompletedEvent.getContext().getCatalog().orElse(""),
                queryCompletedEvent.getStatistics().getTotalRows(),
                queryCompletedEvent.getStatistics().isComplete(),
                queryCompletedEvent.getMetadata().getQuery());
        if (Strings.isNullOrEmpty(queryCompletedEvent.getContext().getUser())) {
            logger.error("QueryId:'%s', Error:'Username is NULL'", queryCompletedEvent.getMetadata().getQueryId());
        }
    }

    @Override
    public void splitCompleted(SplitCompletedEvent splitCompletedEvent)
    {
    }
}
