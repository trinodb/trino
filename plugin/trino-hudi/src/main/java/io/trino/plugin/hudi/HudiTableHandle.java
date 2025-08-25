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
import io.airlift.log.Logger;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.avro.Schema;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private static final Logger log = Logger.get(HudiTableHandle.class);
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HoodieTableType tableType;
    private final List<HiveColumnHandle> partitionColumns;
    // Used only for validation when config property hudi.query-partition-filter-required is enabled
    private final Set<HiveColumnHandle> constraintColumns;
    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;
    private final Optional<Lazy<Schema>> hudiTableSchema;
    // Coordinator-only
    private final transient Optional<Table> table;
    private final transient Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient;
    private final transient Lazy<String> lazyLatestCommitTime;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates,
            @JsonProperty("tableSchemaStr") String tableSchemaStr,
            @JsonProperty("latestCommitTime") String latestCommitTime)
    {
        this(Optional.empty(), Optional.empty(), schemaName, tableName, basePath, tableType, partitionColumns, ImmutableSet.of(),
                partitionPredicates, regularPredicates, buildTableSchema(tableSchemaStr), () -> latestCommitTime);
    }

    public HudiTableHandle(
            Table table,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            Optional<Lazy<Schema>> hudiTableSchema)
    {
        this(
                Optional.of(table),
                Optional.of(lazyMetaClient),
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                constraintColumns,
                partitionPredicates,
                regularPredicates,
                hudiTableSchema,
                () -> lazyMetaClient
                        .get()
                        .getActiveTimeline()
                        .getCommitsTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::requestedTime)
                        .orElseThrow(() -> new TrinoException(
                                HudiErrorCode.HUDI_NO_VALID_COMMIT,
                                "Table has no valid commits")));
    }

    HudiTableHandle(
            Optional<Table> table,
            Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            Optional<Lazy<Schema>> hudiTableSchema,
            Supplier<String> latestCommitTimeSupplier)
    {
        this.table = requireNonNull(table, "table is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "lazyMetaClient is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
        this.hudiTableSchema = requireNonNull(hudiTableSchema, "hudiTableSchema is null");
        this.lazyLatestCommitTime = Lazy.lazily(latestCommitTimeSupplier);
    }

    /**
     * Builds a lazily-parsed Avro schema from the given schema string.
     * <p>
     * Returns {@code Optional.empty()} if the input string is null/empty
     * or if parsing the schema fails.
     */
    private static Optional<Lazy<Schema>> buildTableSchema(String tableSchemaStr)
    {
        if (StringUtils.isNullOrEmpty(tableSchemaStr)) {
            return Optional.empty();
        }

        try {
            Lazy<Schema> lazySchema = Lazy.lazily(() -> new Schema.Parser().parse(tableSchemaStr));
            return Optional.of(lazySchema);
        }
        catch (Exception e) {
            log.warn(e, "Failed to parse table schema: %s", tableSchemaStr);
            return Optional.empty();
        }
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

    @JsonProperty
    public String getLatestCommitTime()
    {
        return lazyLatestCommitTime.get();
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

    @JsonProperty
    public String getTableSchemaStr()
    {
        return hudiTableSchema
                .map(Lazy::get)
                .map(Schema::toString)
                .orElse("");
    }

    @JsonIgnore
    public Schema getTableSchema()
    {
        return hudiTableSchema.map(Lazy::get).orElse(null);
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
                regularPredicates.intersect(regularTupleDomain),
                hudiTableSchema,
                this::getLatestCommitTime);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }
}
