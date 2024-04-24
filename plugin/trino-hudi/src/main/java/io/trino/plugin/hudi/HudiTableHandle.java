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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.model.HudiTableType;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Set;

import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HudiTableType tableType;
    private final List<HiveColumnHandle> partitionColumns;
    // Used only for validation when config property hudi.query-partition-filter-required is enabled
    private final Set<HiveColumnHandle> constraintColumns;
    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HudiTableType tableType,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this(schemaName, tableName, basePath, tableType, partitionColumns, ImmutableSet.of(), partitionPredicates, regularPredicates);
    }

    public HudiTableHandle(
            String schemaName,
            String tableName,
            String basePath,
            HudiTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
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
    public HudiTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    // do not serialize constraint columns as they are not needed on workers
    @JsonIgnore
    public Set<HiveColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getRegularPredicates()
    {
        return regularPredicates;
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName(schemaName, tableName);
    }

    HudiTableHandle applyPredicates(
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionTupleDomain,
            TupleDomain<HiveColumnHandle> regularTupleDomain)
    {
        return new HudiTableHandle(
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                constraintColumns,
                partitionPredicates.intersect(partitionTupleDomain),
                regularPredicates.intersect(regularTupleDomain));
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }
}
