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
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HoodieTableType tableType;
    private final List<HiveColumnHandle> partitionColumns;
    // Used only for validation when config property hudi.query-partition-filter-required is enabled
    private final Set<HiveColumnHandle> constraintColumns;
    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;
    // Coordinator-only
    private final transient Optional<Table> table;
    private final transient Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient;
    private final transient Optional<Lazy<String>> lazyLatestCommitTime;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this(Optional.empty(), Optional.empty(), schemaName, tableName, basePath, tableType, partitionColumns, ImmutableSet.of(), partitionPredicates, regularPredicates);
    }

    public HudiTableHandle(
            Optional<Table> table,
            Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates)
    {
        this.table = requireNonNull(table, "table is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "lazyMetaClient is null");
        this.lazyLatestCommitTime = Optional.of(Lazy.lazily(() ->
                getMetaClient().getActiveTimeline()
                        .getCommitsTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::requestedTime)
                        .orElseThrow(() -> new TrinoException(HudiErrorCode.HUDI_NO_VALID_COMMIT, "Table has no valid commits"))));
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
    }

    public Table getTable()
    {
        checkArgument(table.isPresent(),
                "getTable() called on a table handle that has no metastore table object; "
                        + "this is likely because it is called on the worker.");
        return table.get();
    }

    public HoodieTableMetaClient getMetaClient()
    {
        checkArgument(lazyMetaClient.isPresent(),
                "getMetaClient() called on a table handle that has no Hudi meta-client; "
                        + "this is likely because it is called on the worker.");
        return lazyMetaClient.get().get();
    }

    public String getLatestCommitTime()
    {
        checkArgument(lazyLatestCommitTime.isPresent(),
                "getLatestCommitTime() called on a table handle that has no Hudi meta-client; "
                        + "this is likely because it is called on the worker.");
        return lazyLatestCommitTime.get().get();
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
                table,
                lazyMetaClient,
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
