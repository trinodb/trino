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
package io.trino.plugin.redis;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.decoder.dummy.DummyRowDecoder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.Ranges;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;

import javax.annotation.Nullable;
import javax.inject.Inject;

import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.trino.plugin.redis.RedisSplit.toRedisDataType;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * Manages the Redis connector specific metadata information. The Connector provides an additional set of columns
 * for each table that are created as hidden columns. See {@link RedisInternalFieldDescription} for a list
 * of additional columns.
 */
public class RedisMetadata
        implements ConnectorMetadata
{
    private static final Logger log = Logger.get(RedisMetadata.class);

    private final boolean hideInternalColumns;

    private final Supplier<Map<SchemaTableName, RedisTableDescription>> redisTableDescriptionSupplier;

    @Inject
    RedisMetadata(
            RedisConnectorConfig redisConnectorConfig,
            Supplier<Map<SchemaTableName, RedisTableDescription>> redisTableDescriptionSupplier)
    {
        hideInternalColumns = redisConnectorConfig.isHideInternalColumns();

        log.debug("Loading redis table definitions from %s", redisConnectorConfig.getTableDescriptionDir().getAbsolutePath());

        this.redisTableDescriptionSupplier = Suppliers.memoizeWithExpiration(
                redisTableDescriptionSupplier::get,
                redisConnectorConfig.getTableDescriptionCacheDuration().toMillis(),
                MILLISECONDS);
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        Set<String> schemas = getDefinedTables().keySet().stream()
                .map(SchemaTableName::getSchemaName)
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return ImmutableList.copyOf(schemas);
    }

    @Override
    public RedisTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        RedisTableDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            return null;
        }

        // check if keys are supplied in a zset
        // via the table description doc
        String keyName = null;
        if (table.getKey() != null) {
            keyName = table.getKey().getName();
        }

        return new RedisTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                getDataFormat(table.getKey()),
                getDataFormat(table.getValue()),
                keyName,
                TupleDomain.all());
    }

    private static String getDataFormat(RedisTableFieldGroup fieldGroup)
    {
        return (fieldGroup == null) ? DummyRowDecoder.NAME : fieldGroup.getDataFormat();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        SchemaTableName schemaTableName = ((RedisTableHandle) tableHandle).toSchemaTableName();
        ConnectorTableMetadata tableMetadata = getTableMetadata(schemaTableName);
        if (tableMetadata == null) {
            throw new TableNotFoundException(schemaTableName);
        }
        return tableMetadata;
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (SchemaTableName tableName : getDefinedTables().keySet()) {
            if (schemaName.map(tableName.getSchemaName()::equals).orElse(true)) {
                builder.add(tableName);
            }
        }

        return builder.build();
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        RedisTableHandle redisTableHandle = ((RedisTableHandle) tableHandle);

        RedisTableDescription redisTableDescription = getDefinedTables().get(redisTableHandle.toSchemaTableName());
        if (redisTableDescription == null) {
            throw new TableNotFoundException(redisTableHandle.toSchemaTableName());
        }

        ImmutableMap.Builder<String, ColumnHandle> columnHandles = ImmutableMap.builder();

        int index = 0;
        RedisTableFieldGroup key = redisTableDescription.getKey();
        if (key != null) {
            List<RedisTableFieldDescription> fields = key.getFields();
            if (fields != null) {
                for (RedisTableFieldDescription field : fields) {
                    columnHandles.put(field.getName(), field.getColumnHandle(true, index));
                    index++;
                }
            }
        }

        RedisTableFieldGroup value = redisTableDescription.getValue();
        if (value != null) {
            List<RedisTableFieldDescription> fields = value.getFields();
            if (fields != null) {
                for (RedisTableFieldDescription field : fields) {
                    columnHandles.put(field.getName(), field.getColumnHandle(false, index));
                    index++;
                }
            }
        }

        for (RedisInternalFieldDescription field : RedisInternalFieldDescription.values()) {
            columnHandles.put(field.getColumnName(), field.getColumnHandle(index, hideInternalColumns));
            index++;
        }

        return columnHandles.buildOrThrow();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        RedisTableHandle handle = (RedisTableHandle) table;
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

            // Currently, only Redis key of string type supports pushdown.
            // Key pushdown is not supported when multiple key fields are defined in the table definition file.
            if (toRedisDataType(handle.getKeyDataFormat()) != RedisDataType.STRING) {
                unsupported = domains;
            }
            else if (getUserDefinedKeySize(session, handle) > 1) {
                unsupported = domains;
            }
            else {
                for (Map.Entry<ColumnHandle, Domain> entry : domains.entrySet()) {
                    RedisColumnHandle columnHandle = (RedisColumnHandle) entry.getKey();
                    Domain domain = entry.getValue();
                    if (isColumnSupportsPushdown(columnHandle, domain)) {
                        supported.put(columnHandle, domain);
                    }
                    else {
                        unsupported.put(columnHandle, domain);
                    }
                }
            }

            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new RedisTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getKeyDataFormat(),
                handle.getValueDataFormat(),
                handle.getKeyName(),
                newDomain);

        return Optional.of(new ConstraintApplicationResult<>(handle, remainingFilter, false));
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");

        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> columns = ImmutableMap.builder();

        List<SchemaTableName> tableNames;
        if (prefix.getTable().isEmpty()) {
            tableNames = listTables(session, prefix.getSchema());
        }
        else {
            tableNames = ImmutableList.of(prefix.toSchemaTableName());
        }

        for (SchemaTableName tableName : tableNames) {
            ConnectorTableMetadata tableMetadata = getTableMetadata(tableName);
            // table can disappear during listing operation
            if (tableMetadata != null) {
                columns.put(tableName, tableMetadata.getColumns());
            }
        }
        return columns.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((RedisColumnHandle) columnHandle).getColumnMetadata();
    }

    @VisibleForTesting
    Map<SchemaTableName, RedisTableDescription> getDefinedTables()
    {
        return redisTableDescriptionSupplier.get();
    }

    @Nullable
    private ConnectorTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        RedisTableDescription table = getDefinedTables().get(schemaTableName);
        if (table == null) {
            return null;
        }

        ImmutableList.Builder<ColumnMetadata> builder = ImmutableList.builder();

        appendFields(builder, table.getKey());
        appendFields(builder, table.getValue());

        for (RedisInternalFieldDescription fieldDescription : RedisInternalFieldDescription.values()) {
            builder.add(fieldDescription.getColumnMetadata(hideInternalColumns));
        }

        return new ConnectorTableMetadata(schemaTableName, builder.build());
    }

    private static void appendFields(ImmutableList.Builder<ColumnMetadata> builder, RedisTableFieldGroup group)
    {
        if (group != null) {
            List<RedisTableFieldDescription> fields = group.getFields();
            if (fields != null) {
                for (RedisTableFieldDescription fieldDescription : fields) {
                    builder.add(fieldDescription.getColumnMetadata());
                }
            }
        }
    }

    private boolean isColumnSupportsPushdown(RedisColumnHandle columnHandle, Domain domain)
    {
        if (columnHandle.isKeyDecoder()) {
            if (domain.isSingleValue()) {
                return true;
            }
            ValueSet valueSet = domain.getValues();
            if (valueSet instanceof SortedRangeSet) {
                Ranges ranges = ((SortedRangeSet) valueSet).getRanges();
                List<Range> rangeList = ranges.getOrderedRanges();
                return rangeList.stream().allMatch(Range::isSingleValue);
            }
        }
        return false;
    }

    private long getUserDefinedKeySize(ConnectorSession session, RedisTableHandle handle)
    {
        return getColumnHandles(session, handle).values().stream()
                .filter(column -> ((RedisColumnHandle) column).isKeyDecoder())
                .count();
    }
}
