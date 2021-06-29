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
package io.trino.plugin.pinot;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.query.AggregationExpression;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.DynamicTableBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableProperties;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.ArrayType;
import org.apache.pinot.spi.data.Schema;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.pinot.PinotColumn.getPinotColumnsForPinotSchema;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.BIGINT_AGGREGATIONS;
import static io.trino.plugin.pinot.query.DynamicTableBuilder.DOUBLE_AGGREGATIONS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class PinotMetadata
        implements ConnectorMetadata
{
    private static final Object ALL_TABLES_CACHE_KEY = new Object();
    private static final String SCHEMA_NAME = "default";
    private static final String PINOT_COLUMN_NAME_PROPERTY = "pinotColumnName";

    private final LoadingCache<String, List<PinotColumn>> pinotTableColumnCache;
    private final LoadingCache<Object, List<String>> allTablesCache;

    @Inject
    public PinotMetadata(
            PinotClient pinotClient,
            PinotConfig pinotConfig,
            @ForPinot Executor executor)
    {
        requireNonNull(pinotConfig, "pinot config");
        long metadataCacheExpiryMillis = pinotConfig.getMetadataCacheExpiry().roundTo(TimeUnit.MILLISECONDS);
        this.allTablesCache = CacheBuilder.newBuilder()
                .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(pinotClient::getAllTables), executor));
        this.pinotTableColumnCache =
                CacheBuilder.newBuilder()
                        .refreshAfterWrite(metadataCacheExpiryMillis, TimeUnit.MILLISECONDS)
                        .build(asyncReloading(new CacheLoader<>()
                        {
                            @Override
                            public List<PinotColumn> load(String tableName)
                                    throws Exception
                            {
                                Schema tablePinotSchema = pinotClient.getTableSchema(tableName);
                                return getPinotColumnsForPinotSchema(tablePinotSchema);
                            }
                        }, executor));

        executor.execute(() -> this.allTablesCache.refresh(ALL_TABLES_CACHE_KEY));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.of(SCHEMA_NAME);
    }

    @Override
    public PinotTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        if (tableName.getTableName().trim().startsWith("select ")) {
            DynamicTable dynamicTable = DynamicTableBuilder.buildFromPql(this, tableName);
            return new PinotTableHandle(tableName.getSchemaName(), dynamicTable.getTableName(), TupleDomain.all(), OptionalLong.empty(), Optional.of(dynamicTable));
        }
        return new PinotTableHandle(tableName.getSchemaName(), tableName.getTableName());
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) table;
        if (pinotTableHandle.getQuery().isPresent()) {
            DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
            Map<String, ColumnHandle> columnHandles = getColumnHandles(session, table);
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder = ImmutableList.builder();
            for (String columnName : dynamicTable.getSelections()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (String columnName : dynamicTable.getGroupingColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }

            for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
                PinotColumnHandle pinotColumnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH));
                columnMetadataBuilder.add(pinotColumnHandle.getColumnMetadata());
            }
            SchemaTableName schemaTableName = new SchemaTableName(pinotTableHandle.getSchemaName(), dynamicTable.getTableName());
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        }
        SchemaTableName tableName = new SchemaTableName(pinotTableHandle.getSchemaName(), pinotTableHandle.getTableName());

        return getTableMetadata(tableName);
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaNameOrNull)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String table : getPinotTableNames()) {
            builder.add(new SchemaTableName(SCHEMA_NAME, table));
        }
        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        if (pinotTableHandle.getQuery().isPresent()) {
            return getDynamicTableColumnHandles(pinotTableHandle);
        }
        return getPinotColumnHandles(pinotTableHandle.getTableName());
    }

    public Map<String, ColumnHandle> getPinotColumnHandles(String tableName)
    {
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (ColumnMetadata columnMetadata : getColumnsMetadata(tableName)) {
            columnHandlesBuilder.put(columnMetadata.getName(),
                    new PinotColumnHandle(getPinotColumnName(columnMetadata), columnMetadata.getType()));
        }
        return columnHandlesBuilder.build();
    }

    private static String getPinotColumnName(ColumnMetadata columnMetadata)
    {
        Object pinotColumnName = requireNonNull(columnMetadata.getProperties().get(PINOT_COLUMN_NAME_PROPERTY), "Pinot column name is missing");
        return pinotColumnName.toString();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();
        for (SchemaTableName tableName : listTables(session, prefix)) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            ColumnHandle columnHandle)
    {
        return ((PinotColumnHandle) columnHandle).getColumnMetadata();
    }

    @Override
    public Optional<Object> getInfo(ConnectorTableHandle table)
    {
        return Optional.empty();
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle table, long limit)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        if (handle.getLimit().isPresent() && handle.getLimit().getAsLong() <= limit) {
            return Optional.empty();
        }
        Optional<DynamicTable> dynamicTable = handle.getQuery();
        if (dynamicTable.isPresent() &&
                (dynamicTable.get().getLimit().isEmpty() || dynamicTable.get().getLimit().getAsLong() > limit)) {
            dynamicTable = Optional.of(new DynamicTable(dynamicTable.get().getTableName(),
                    dynamicTable.get().getSuffix(),
                    dynamicTable.get().getSelections(),
                    dynamicTable.get().getGroupingColumns(),
                    dynamicTable.get().getFilter(),
                    dynamicTable.get().getAggregateColumns(),
                    dynamicTable.get().getOrderBy(),
                    OptionalLong.of(limit),
                    dynamicTable.get().getOffset(),
                    dynamicTable.get().getQuery()));
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                OptionalLong.of(limit),
                dynamicTable);
        boolean singleSplit = dynamicTable.isPresent();
        return Optional.of(new LimitApplicationResult<>(handle, singleSplit, false));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        PinotTableHandle handle = (PinotTableHandle) table;
        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();

        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();

            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                // Pinot does not support array literals
                if (((PinotColumnHandle) entry.getKey()).getDataType() instanceof ArrayType) {
                    unsupported.put(entry.getKey(), entry.getValue());
                }
                else {
                    supported.put(entry.getKey(), entry.getValue());
                }
            }
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new PinotTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getLimit(),
                handle.getQuery());
        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, false));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @VisibleForTesting
    public List<PinotColumn> getPinotColumns(String tableName)
    {
        String pinotTableName = getPinotTableNameFromTrinoTableName(tableName);
        return getFromCache(pinotTableColumnCache, pinotTableName);
    }

    private List<String> getPinotTableNames()
    {
        return getFromCache(allTablesCache, ALL_TABLES_CACHE_KEY);
    }

    private static <K, V> V getFromCache(LoadingCache<K, V> cache, K key)
    {
        V value = cache.getIfPresent(key);
        if (value != null) {
            return value;
        }
        try {
            return cache.get(key);
        }
        catch (ExecutionException e) {
            throw new PinotException(PinotErrorCode.PINOT_UNCLASSIFIED_ERROR, Optional.empty(), "Cannot fetch from cache " + key, e.getCause());
        }
    }

    private String getPinotTableNameFromTrinoTableName(String trinoTableName)
    {
        List<String> allTables = getPinotTableNames();
        String pinotTableName = null;
        for (String candidate : allTables) {
            if (trinoTableName.equalsIgnoreCase(candidate)) {
                pinotTableName = candidate;
                break;
            }
        }
        if (pinotTableName == null) {
            throw new TableNotFoundException(new SchemaTableName(SCHEMA_NAME, trinoTableName));
        }
        return pinotTableName;
    }

    private Map<String, ColumnHandle> getDynamicTableColumnHandles(PinotTableHandle pinotTableHandle)
    {
        checkState(pinotTableHandle.getQuery().isPresent(), "dynamic table not present");
        String schemaName = pinotTableHandle.getSchemaName();
        DynamicTable dynamicTable = pinotTableHandle.getQuery().get();

        Map<String, ColumnHandle> columnHandles = getPinotColumnHandles(dynamicTable.getTableName());
        ImmutableMap.Builder<String, ColumnHandle> columnHandlesBuilder = ImmutableMap.builder();
        for (String columnName : dynamicTable.getSelections()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (String columnName : dynamicTable.getGroupingColumns()) {
            PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(columnName.toLowerCase(ENGLISH));
            if (columnHandle == null) {
                throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), columnName);
            }
            columnHandlesBuilder.put(columnName.toLowerCase(ENGLISH), columnHandle);
        }

        for (AggregationExpression aggregationExpression : dynamicTable.getAggregateColumns()) {
            if (DOUBLE_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), DOUBLE));
            }
            else if (BIGINT_AGGREGATIONS.contains(aggregationExpression.getAggregationType().toLowerCase(ENGLISH))) {
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(), BIGINT));
            }
            else {
                PinotColumnHandle columnHandle = (PinotColumnHandle) columnHandles.get(aggregationExpression.getBaseColumnName().toLowerCase(ENGLISH));
                if (columnHandle == null) {
                    throw new ColumnNotFoundException(new SchemaTableName(schemaName, dynamicTable.getTableName()), aggregationExpression.getBaseColumnName());
                }
                columnHandlesBuilder.put(aggregationExpression.getOutputColumnName().toLowerCase(ENGLISH),
                        new PinotColumnHandle(aggregationExpression.getOutputColumnName(),
                                columnHandle.getDataType()));
            }
        }
        return columnHandlesBuilder.build();
    }

    private ConnectorTableMetadata getTableMetadata(SchemaTableName tableName)
    {
        return new ConnectorTableMetadata(tableName, getColumnsMetadata(tableName.getTableName()));
    }

    private List<ColumnMetadata> getColumnsMetadata(String tableName)
    {
        List<PinotColumn> columns = getPinotColumns(tableName);
        return columns.stream()
                .map(PinotMetadata::createPinotColumnMetadata)
                .collect(toImmutableList());
    }

    private static ColumnMetadata createPinotColumnMetadata(PinotColumn pinotColumn)
    {
        return ColumnMetadata.builder()
                .setName(pinotColumn.getName().toLowerCase(ENGLISH))
                .setType(pinotColumn.getType())
                .setProperties(ImmutableMap.<String, Object>builder()
                        .put(PINOT_COLUMN_NAME_PROPERTY, pinotColumn.getName())
                        .build())
                .build();
    }

    private List<SchemaTableName> listTables(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (prefix.getSchema().isEmpty() || prefix.getTable().isEmpty()) {
            return listTables(session, Optional.empty());
        }
        return ImmutableList.of(new SchemaTableName(prefix.getSchema().get(), prefix.getTable().get()));
    }
}
