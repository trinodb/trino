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
package io.trino.spi.eventlistener;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.trino.spi.Unstable;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

/**
 * This class is JSON serializable for convenience and serialization compatibility is not guaranteed across versions.
 */
public class QueryMetadata
{
    private final String queryId;
    private final Optional<String> transactionId;
    private final Optional<String> encoding;

    private final String query;
    private final Optional<String> updateType;
    private final Optional<String> preparedQuery;
    private final String queryState;

    private final URI uri;

    private final List<TableInfo> tables;
    private final List<RoutineInfo> routines;

    private final Optional<String> plan;
    private final Optional<String> jsonPlan;

    private final Supplier<Optional<String>> payloadProvider;

    @JsonCreator
    @Unstable
    public QueryMetadata(
            String queryId,
            Optional<String> transactionId,
            Optional<String> encoding,
            String query,
            Optional<String> updateType,
            Optional<String> preparedQuery,
            String queryState,
            List<TableInfo> tables,
            List<RoutineInfo> routines,
            URI uri,
            Optional<String> plan,
            Optional<String> jsonPlan,
            Optional<String> payload)
    {
        this(
                queryId,
                transactionId,
                encoding,
                query,
                updateType,
                preparedQuery,
                queryState,
                tables,
                routines,
                uri,
                plan,
                jsonPlan,
                () -> payload);
    }

    public QueryMetadata(
            String queryId,
            Optional<String> transactionId,
            Optional<String> encoding,
            String query,
            Optional<String> updateType,
            Optional<String> preparedQuery,
            String queryState,
            List<TableInfo> tables,
            List<RoutineInfo> routines,
            URI uri,
            Optional<String> plan,
            Optional<String> jsonPlan,
            Supplier<Optional<String>> payloadProvider)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.encoding = requireNonNull(encoding, "encoding is null");
        this.query = requireNonNull(query, "query is null");
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.preparedQuery = requireNonNull(preparedQuery, "preparedQuery is null");
        this.queryState = requireNonNull(queryState, "queryState is null");
        this.tables = requireNonNull(tables, "tables is null");
        this.routines = requireNonNull(routines, "routines is null");
        this.uri = requireNonNull(uri, "uri is null");
        this.plan = requireNonNull(plan, "plan is null");
        this.jsonPlan = requireNonNull(jsonPlan, "jsonPlan is null");
        this.payloadProvider = requireNonNull(payloadProvider, "payloadProvider is null");
    }

    @JsonProperty
    public String getQueryId()
    {
        return queryId;
    }

    @JsonProperty
    public Optional<String> getTransactionId()
    {
        return transactionId;
    }

    @JsonProperty
    public Optional<String> getEncoding()
    {
        return encoding;
    }

    @JsonProperty
    public String getQuery()
    {
        return query;
    }

    @JsonProperty
    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public Optional<String> getPreparedQuery()
    {
        return preparedQuery;
    }

    @JsonProperty
    public String getQueryState()
    {
        return queryState;
    }

    @JsonProperty
    public List<TableInfo> getTables()
    {
        return tables;
    }

    @JsonProperty
    public List<RoutineInfo> getRoutines()
    {
        return routines;
    }

    @JsonProperty
    public URI getUri()
    {
        return uri;
    }

    @JsonProperty
    public Optional<String> getPlan()
    {
        return plan;
    }

    @JsonProperty
    public Optional<String> getJsonPlan()
    {
        return jsonPlan;
    }

    @JsonProperty
    public Optional<String> getPayload()
    {
        return payloadProvider.get();
    }
}
