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
package io.trino.plugin.hudi;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;

import java.util.List;
import java.util.Optional;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HoodieTableType tableType;

    private final List<HiveColumnHandle> dataColumns;

    private final List<HiveColumnHandle> partitionColumns;
    private final Optional<String> startVersion;
    private final Optional<String> endVersion;

    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("dataColumns") List<HiveColumnHandle> dataColumns,
            @JsonProperty("startVersion") Optional<String> startVersion,
            @JsonProperty("endVersion") Optional<String> endVersion,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
        this.dataColumns = ImmutableList.copyOf(requireNonNull(dataColumns, "dataColumns is null"));
        this.startVersion = requireNonNull(startVersion, "startVersion is null");
        this.endVersion = requireNonNull(endVersion, "endVersion is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
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
    public String getBasePath()
    {
        return basePath;
    }

    @JsonProperty
    public HoodieTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getRegularPredicates()
    {
        return regularPredicates;
    }

    @JsonProperty
    public Optional<String> getStartVersion()
    {
        return startVersion;
    }

    @JsonProperty
    public Optional<String> getEndVersion()
    {
        return endVersion;
    }

    @JsonProperty
    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName(schemaName, tableName);
    }

    @JsonProperty
    public List<HiveColumnHandle> getDataColumns()
    {
        return dataColumns;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    HudiTableHandle applyPredicates(
            TupleDomain<HiveColumnHandle> partitionTupleDomain,
            TupleDomain<HiveColumnHandle> regularTupleDomain)
    {
        return new HudiTableHandle(
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                dataColumns,
                startVersion,
                endVersion,
                partitionPredicates.intersect(partitionTupleDomain),
                regularPredicates.intersect(regularTupleDomain));
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }
}
