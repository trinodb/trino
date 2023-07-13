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
package io.trino.plugin.deltalake;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.spi.connector.ConnectorOutputTableHandle;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.PARTITION_KEY;
import static java.util.Objects.requireNonNull;

public class DeltaLakeOutputTableHandle
        implements ConnectorOutputTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final List<DeltaLakeColumnHandle> inputColumns;
    private final String location;
    private final Optional<Long> checkpointInterval;
    private final boolean external;
    private final Optional<String> comment;
    private final Optional<Boolean> changeDataFeedEnabled;
    private final String schemaString;
    private final ProtocolEntry protocolEntry;

    @JsonCreator
    public DeltaLakeOutputTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("inputColumns") List<DeltaLakeColumnHandle> inputColumns,
            @JsonProperty("location") String location,
            @JsonProperty("checkpointInterval") Optional<Long> checkpointInterval,
            @JsonProperty("external") boolean external,
            @JsonProperty("comment") Optional<String> comment,
            @JsonProperty("changeDataFeedEnabled") Optional<Boolean> changeDataFeedEnabled,
            @JsonProperty("schemaString") String schemaString,
            @JsonProperty("protocolEntry") ProtocolEntry protocolEntry)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.inputColumns = ImmutableList.copyOf(inputColumns);
        this.location = requireNonNull(location, "location is null");
        this.checkpointInterval = checkpointInterval;
        this.external = external;
        this.comment = requireNonNull(comment, "comment is null");
        this.changeDataFeedEnabled = requireNonNull(changeDataFeedEnabled, "changeDataFeedEnabled is null");
        this.schemaString = requireNonNull(schemaString, "schemaString is null");
        this.protocolEntry = requireNonNull(protocolEntry, "protocolEntry is null");
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public List<DeltaLakeColumnHandle> getInputColumns()
    {
        return inputColumns;
    }

    @JsonProperty
    public String getLocation()
    {
        return location;
    }

    @JsonIgnore
    public List<String> getPartitionedBy()
    {
        return getInputColumns().stream()
                .filter(column -> column.getColumnType() == PARTITION_KEY)
                .map(DeltaLakeColumnHandle::getColumnName)
                .collect(toImmutableList());
    }

    @JsonProperty
    public Optional<Long> getCheckpointInterval()
    {
        return checkpointInterval;
    }

    @JsonProperty
    public boolean isExternal()
    {
        return external;
    }

    @JsonProperty
    public Optional<String> getComment()
    {
        return comment;
    }

    @JsonProperty
    public Optional<Boolean> getChangeDataFeedEnabled()
    {
        return changeDataFeedEnabled;
    }

    @JsonProperty
    public String getSchemaString()
    {
        return schemaString;
    }

    @JsonProperty
    public ProtocolEntry getProtocolEntry()
    {
        return protocolEntry;
    }
}
