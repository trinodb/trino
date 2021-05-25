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
package io.trino.plugin.queryeventlistener.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.eventlistener.QueryCompletedEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CompletedEvent
{
    private static final String KEY_EVENT_TYPE = "eventType";
    private static final String KEY_QUERY_ID = "queryId";
    private static final String KEY_CREATE_TIME = "createTime";
    private static final String KEY_QUEUED_TIME = "queuedTime";
    private static final String KEY_WALL_TIME = "wallTime";
    private static final String KEY_CPU_TIME = "cpuTime";
    private static final String KEY_END_TIME = "endTime";
    private static final String KEY_USER = "user";
    private static final String KEY_SOURCE = "source";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_CATALOG = "catalog";
    private static final String KEY_RECORDS = "records";
    private static final String KEY_COMPLETED = "completed";
    private static final String KEY_SQL = "sql";

    @JsonProperty(KEY_EVENT_TYPE)
    private final String eventType = "QueryCompleted";

    @JsonProperty(KEY_QUERY_ID)
    private final String queryId;

    @JsonProperty(KEY_CREATE_TIME)
    private final LocalDateTime createTime;

    @JsonProperty(KEY_QUEUED_TIME)
    private final long queuedTime;

    @JsonProperty(KEY_WALL_TIME)
    private final long wallTime;

    @JsonProperty(KEY_CPU_TIME)
    private final long cpuTime;

    @JsonProperty(KEY_END_TIME)
    private final LocalDateTime endTime;

    @JsonProperty(KEY_USER)
    private final String user;

    @JsonProperty(KEY_SOURCE)
    private final String source;

    @JsonProperty(KEY_SCHEMA)
    private final String schema;

    @JsonProperty(KEY_CATALOG)
    private final String catalog;

    @JsonProperty(KEY_RECORDS)
    private final long records;

    @JsonProperty(KEY_COMPLETED)
    private final boolean completed;

    @JsonProperty(KEY_SQL)
    private final String sql;

    public CompletedEvent(QueryCompletedEvent queryCompletedEvent)
    {
        this.queryId = queryCompletedEvent.getMetadata().getQueryId();
        this.createTime = LocalDateTime.ofInstant(queryCompletedEvent.getCreateTime(), ZoneOffset.systemDefault());
        this.queuedTime = queryCompletedEvent.getStatistics().getQueuedTime().toMillis();
        this.wallTime = queryCompletedEvent.getStatistics().getWallTime().toMillis();
        this.cpuTime = queryCompletedEvent.getStatistics().getCpuTime().toMillis();
        this.endTime = LocalDateTime.ofInstant(queryCompletedEvent.getEndTime(), ZoneOffset.systemDefault());
        this.user = queryCompletedEvent.getContext().getUser();
        this.source = queryCompletedEvent.getContext().getSource().orElse(null);
        this.schema = queryCompletedEvent.getContext().getSchema().orElse(null);
        this.catalog = queryCompletedEvent.getContext().getCatalog().orElse(null);
        this.records = queryCompletedEvent.getStatistics().getTotalRows();
        this.completed = queryCompletedEvent.getStatistics().isComplete();
        this.sql = queryCompletedEvent.getMetadata().getQuery();
    }

    @JsonCreator
    private CompletedEvent(
            @JsonProperty(KEY_EVENT_TYPE) String eventType,
            @JsonProperty(KEY_QUERY_ID) String queryId,
            @JsonProperty(KEY_CREATE_TIME) LocalDateTime createTime,
            @JsonProperty(KEY_QUEUED_TIME) Long queuedTime,
            @JsonProperty(KEY_WALL_TIME) Long wallTime,
            @JsonProperty(KEY_CPU_TIME) Long cpuTime,
            @JsonProperty(KEY_END_TIME) LocalDateTime endTime,
            @JsonProperty(KEY_USER) String user,
            @JsonProperty(KEY_SOURCE) String source,
            @JsonProperty(KEY_SCHEMA) String schema,
            @JsonProperty(KEY_CATALOG) String catalog,
            @JsonProperty(KEY_RECORDS) Long records,
            @JsonProperty(KEY_COMPLETED) Boolean completed,
            @JsonProperty(KEY_SQL) String sql)
    {
        this.queryId = queryId;
        this.createTime = createTime;
        this.queuedTime = queuedTime;
        this.wallTime = wallTime;
        this.cpuTime = cpuTime;
        this.endTime = endTime;
        this.user = user;
        this.source = source;
        this.schema = schema;
        this.catalog = catalog;
        this.records = (records == null) ? -1L : records;
        this.completed = completed != null && completed;
        this.sql = sql;
    }

    public String getEventType()
    {
        return eventType;
    }

    public String getQueryId()
    {
        return queryId;
    }

    public LocalDateTime getCreateTime()
    {
        return createTime;
    }

    public long getQueuedTime()
    {
        return queuedTime;
    }

    public long getWallTime()
    {
        return wallTime;
    }

    public long getCpuTime()
    {
        return cpuTime;
    }

    public LocalDateTime getEndTime()
    {
        return endTime;
    }

    public String getUser()
    {
        return user;
    }

    public String getSource()
    {
        return source;
    }

    public String getSchema()
    {
        return schema;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public long getRecords()
    {
        return records;
    }

    public boolean isCompleted()
    {
        return completed;
    }

    public String getSql()
    {
        return sql;
    }

    @Override
    public String toString()
    {
        return "CompletedEvent{" +
                "eventType='" + eventType + '\'' +
                ", queryId='" + queryId + '\'' +
                ", createTime=" + createTime +
                ", queuedTime=" + queuedTime +
                ", wallTime=" + wallTime +
                ", cpuTime=" + cpuTime +
                ", endTime=" + endTime +
                ", user='" + user + '\'' +
                ", source='" + source + '\'' +
                ", schema='" + schema + '\'' +
                ", catalog='" + catalog + '\'' +
                ", records=" + records +
                ", completed=" + completed +
                ", sql='" + sql + '\'' +
                '}';
    }
}
