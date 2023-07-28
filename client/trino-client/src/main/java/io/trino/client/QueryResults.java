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
package io.trino.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.errorprone.annotations.Immutable;
import jakarta.annotation.Nullable;

import java.net.URI;
import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.trino.client.QueryResultSetFormatResolver.formatNameForClass;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * QueryResults implementations are shared between the client and the server.
 * Implementing class is responsible for transferring result data over the wire (through @JsonProperty)
 * and deserializing incoming result data (through @JsonCreator) according to the QueryData interface.
 */
@Immutable
public abstract class QueryResults
        implements QueryStatusInfo, QueryData
{
    private final String id;
    private final URI infoUri;
    private final URI partialCancelUri;
    private final URI nextUri;
    private final List<Column> columns;
    private final StatementStats stats;
    private final QueryError error;
    private final List<Warning> warnings;
    private final String updateType;
    private final Long updateCount;

    public QueryResults(QueryResults from)
    {
        this(
                from.id,
                from.infoUri,
                from.partialCancelUri,
                from.nextUri,
                from.columns,
                from.stats,
                from.error,
                from.warnings,
                from.updateType,
                from.updateCount);
    }

    @JsonCreator
    public QueryResults(
            @JsonProperty("id") String id,
            @JsonProperty("infoUri") URI infoUri,
            @JsonProperty("partialCancelUri") URI partialCancelUri,
            @JsonProperty("nextUri") URI nextUri,
            @JsonProperty("columns") List<Column> columns,
            @JsonProperty("stats") StatementStats stats,
            @JsonProperty("error") QueryError error,
            @JsonProperty("warnings") List<Warning> warnings,
            @JsonProperty("updateType") String updateType,
            @JsonProperty("updateCount") Long updateCount)
    {
        this.id = requireNonNull(id, "id is null");
        this.infoUri = requireNonNull(infoUri, "infoUri is null");
        this.partialCancelUri = partialCancelUri;
        this.nextUri = nextUri;
        this.columns = (columns != null) ? ImmutableList.copyOf(columns) : null;
        this.stats = requireNonNull(stats, "stats is null");
        this.error = error;
        this.warnings = warnings != null ? ImmutableList.copyOf(requireNonNull(warnings, "warnings is null")) : ImmutableList.of();
        this.updateType = updateType;
        this.updateCount = updateCount;
    }

    @JsonProperty
    @Override
    public String getId()
    {
        return id;
    }

    @JsonProperty
    @Override
    public URI getInfoUri()
    {
        return infoUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getPartialCancelUri()
    {
        return partialCancelUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public URI getNextUri()
    {
        return nextUri;
    }

    @Nullable
    @JsonProperty
    @Override
    public List<Column> getColumns()
    {
        return columns;
    }

    @JsonProperty
    @Override
    public StatementStats getStats()
    {
        return stats;
    }

    @Nullable
    @JsonProperty
    @Override
    public QueryError getError()
    {
        return error;
    }

    @JsonProperty
    @Override
    public List<Warning> getWarnings()
    {
        return warnings;
    }

    @Nullable
    @JsonProperty
    @Override
    public String getUpdateType()
    {
        return updateType;
    }

    @Nullable
    @JsonProperty
    @Override
    public Long getUpdateCount()
    {
        return updateCount;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("id", id)
                .add("infoUri", infoUri)
                .add("partialCancelUri", partialCancelUri)
                .add("nextUri", nextUri)
                .add("columns", columns)
                .add("stats", stats)
                .add("error", error)
                .add("updateType", updateType)
                .add("updateCount", updateCount)
                .add("hasData", hasData())
                .add("dataFormat", formatNameForClass(this.getClass()))
                .toString();
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        protected String id;
        protected URI infoUri;
        protected URI partialCancelUri;
        protected URI nextUri;
        protected List<Column> columns;
        protected StatementStats stats;
        protected QueryError error;
        protected List<Warning> warnings = ImmutableList.of();
        protected String updateType;
        protected Long updateCount;

        public Builder withQueryId(String id)
        {
            this.id = id;
            return this;
        }

        public Builder withInfoUri(URI infoUri)
        {
            this.infoUri = infoUri;
            return this;
        }

        public Builder withPartialCancelUri(URI partialCancelUri)
        {
            this.partialCancelUri = partialCancelUri;
            return this;
        }

        public Builder withNextUri(URI nextUri)
        {
            this.nextUri = nextUri;
            return this;
        }

        public Builder withColumns(List<Column> columns)
        {
            this.columns = columns;
            return this;
        }

        public Builder withStats(StatementStats stats)
        {
            this.stats = stats;
            return this;
        }

        public Builder withError(QueryError error)
        {
            this.error = error;
            return this;
        }

        public Builder withWarnings(List<Warning> warnings)
        {
            this.warnings = warnings;
            return this;
        }

        public Builder withUpdateType(String updateType)
        {
            this.updateType = updateType;
            return this;
        }

        public Builder withUpdateCount(Long updateCount)
        {
            this.updateCount = updateCount;
            return this;
        }

        public QueryResults buildEmpty()
        {
            return new EmptyQueryResults(id, infoUri, partialCancelUri, nextUri, columns, stats, error, warnings, updateType, updateCount);
        }
    }

    /**
     * This class contains all of the basic fields from QueryResults but carries no result rows.
     */
    @QueryResultsFormat(formatName = "json")
    private static class EmptyQueryResults
            extends QueryResults
    {
        public EmptyQueryResults(
                String id,
                URI infoUri,
                URI partialCancelUri,
                URI nextUri,
                List<Column> columns,
                StatementStats stats,
                QueryError error,
                List<Warning> warnings,
                String updateType,
                Long updateCount)
        {
            super(id, infoUri, partialCancelUri, nextUri, columns, stats, error, warnings, updateType, updateCount);
        }

        @Override
        public Iterable<List<Object>> getData()
        {
            return emptyList();
        }

        @Override
        public boolean hasData()
        {
            return false;
        }
    }
}
