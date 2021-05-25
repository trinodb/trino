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
import io.trino.spi.eventlistener.QueryCreatedEvent;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class CreatedEvent
{
    private static final String KEY_EVENT_TYPE = "eventType";
    private static final String KEY_QUERY_ID = "queryId";
    private static final String KEY_CREATE_TIME = "createTime";
    private static final String KEY_USER = "user";
    private static final String KEY_PRINCIPAL = "principal";
    private static final String KEY_SOURCE = "source";
    private static final String KEY_SCHEMA = "schema";
    private static final String KEY_CATALOG = "catalog";
    private static final String KEY_SQL = "sql";

    @JsonProperty(KEY_EVENT_TYPE)
    private final String eventType = "QueryCreated";

    @JsonProperty(KEY_QUERY_ID)
    private final String queryId;

    @JsonProperty(KEY_CREATE_TIME)
    private final LocalDateTime createTime;

    @JsonProperty(KEY_USER)
    private final String user;

    @JsonProperty(KEY_PRINCIPAL)
    private final String principal;

    @JsonProperty(KEY_SOURCE)
    private final String source;

    @JsonProperty(KEY_SCHEMA)
    private final String schema;

    @JsonProperty(KEY_CATALOG)
    private final String catalog;

    @JsonProperty(KEY_SQL)
    private final String sql;

    public CreatedEvent(QueryCreatedEvent queryCreatedEvent)
    {
        this.queryId = queryCreatedEvent.getMetadata().getQueryId();
        this.createTime = LocalDateTime.ofInstant(queryCreatedEvent.getCreateTime(), ZoneOffset.systemDefault());
        this.user = queryCreatedEvent.getContext().getUser();
        this.principal = queryCreatedEvent.getContext().getPrincipal().orElse(null);
        this.source = queryCreatedEvent.getContext().getSource().orElse(null);
        this.schema = queryCreatedEvent.getContext().getSchema().orElse(null);
        this.catalog = queryCreatedEvent.getContext().getCatalog().orElse(null);
        this.sql = queryCreatedEvent.getMetadata().getQuery();
    }

    @JsonCreator
    private CreatedEvent(
            @JsonProperty(KEY_EVENT_TYPE) String eventType,
            @JsonProperty(KEY_QUERY_ID) String queryId,
            @JsonProperty(KEY_CREATE_TIME) LocalDateTime createTime,
            @JsonProperty(KEY_USER) String user,
            @JsonProperty(KEY_PRINCIPAL) String principal,
            @JsonProperty(KEY_SOURCE) String source,
            @JsonProperty(KEY_SCHEMA) String schema,
            @JsonProperty(KEY_CATALOG) String catalog,
            @JsonProperty(KEY_SQL) String sql)
    {
        this.queryId = queryId;
        this.createTime = createTime;
        this.user = user;
        this.principal = principal;
        this.source = source;
        this.schema = schema;
        this.catalog = catalog;
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

    public String getUser()
    {
        return user;
    }

    public String getPrincipal()
    {
        return principal;
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

    public String getSql()
    {
        return sql;
    }

    @Override
    public String toString()
    {
        return "CreatedEvent{" +
                "eventType='" + eventType + '\'' +
                ", queryId='" + queryId + '\'' +
                ", createTime=" + createTime +
                ", user='" + user + '\'' +
                ", principal='" + principal + '\'' +
                ", source='" + source + '\'' +
                ", schema='" + schema + '\'' +
                ", catalog='" + catalog + '\'' +
                ", sql='" + sql + '\'' +
                '}';
    }
}
