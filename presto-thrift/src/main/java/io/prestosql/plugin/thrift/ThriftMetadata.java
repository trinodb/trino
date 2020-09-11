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
package io.prestosql.plugin.thrift;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.drift.TException;
import io.airlift.drift.client.DriftClient;
import io.airlift.units.Duration;
import io.prestosql.plugin.thrift.annotations.ForMetadataRefresh;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableSchemaName;
import io.prestosql.plugin.thrift.api.PrestoThriftNullableTableMetadata;
import io.prestosql.plugin.thrift.api.PrestoThriftSchemaTableName;
import io.prestosql.plugin.thrift.api.PrestoThriftService;
import io.prestosql.plugin.thrift.api.PrestoThriftServiceException;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.Assignment;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorResolvedIndex;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.ProjectionApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.connector.TableNotFoundException;
import io.prestosql.spi.expression.ConnectorExpression;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;

import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.prestosql.plugin.thrift.ThriftErrorCode.THRIFT_SERVICE_INVALID_RESPONSE;
import static io.prestosql.plugin.thrift.util.ThriftExceptions.toPrestoException;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.function.Function.identity;

public class ThriftMetadata
        implements ConnectorMetadata
{
    private static final Duration EXPIRE_AFTER_WRITE = new Duration(10, MINUTES);
    private static final Duration REFRESH_AFTER_WRITE = new Duration(2, MINUTES);

    private final DriftClient<PrestoThriftService> client;
    private final ThriftHeaderProvider thriftHeaderProvider;
    private final TypeManager typeManager;
    private final LoadingCache<SchemaTableName, Optional<ThriftTableMetadata>> tableCache;

    @Inject
    public ThriftMetadata(
            DriftClient<PrestoThriftService> client,
            ThriftHeaderProvider thriftHeaderProvider,
            TypeManager typeManager,
            @ForMetadataRefresh Executor metadataRefreshExecutor)
    {
        this.client = requireNonNull(client, "client is null");
        this.thriftHeaderProvider = requireNonNull(thriftHeaderProvider, "thriftHeaderProvider is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.tableCache = CacheBuilder.newBuilder()
                .expireAfterWrite(EXPIRE_AFTER_WRITE.toMillis(), MILLISECONDS)
                .refreshAfterWrite(REFRESH_AFTER_WRITE.toMillis(), MILLISECONDS)
                .build(asyncReloading(CacheLoader.from(this::getTableMetadataInternal), metadataRefreshExecutor));
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        try {
            return client.get(thriftHeaderProvider.getHeaders(session)).listSchemaNames();
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }

    @Override
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        return tableCache.getUnchecked(tableName)
                .map(ThriftTableMetadata::getSchemaTableName)
                .map(ThriftTableHandle::new)
                .orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ThriftTableHandle handle = ((ThriftTableHandle) tableHandle);
        return getRequiredTableMetadata(new SchemaTableName(handle.getSchemaName(), handle.getTableName())).toConnectorTableMetadata();
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        try {
            return client.get(thriftHeaderProvider.getHeaders(session)).listTables(new PrestoThriftNullableSchemaName(schemaName.orElse(null))).stream()
                    .map(PrestoThriftSchemaTableName::toSchemaTableName)
                    .collect(toImmutableList());
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(session, tableHandle).getColumns().stream().collect(toImmutableMap(ColumnMetadata::getName, ThriftColumnHandle::new));
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return ((ThriftColumnHandle) columnHandle).toColumnMetadata();
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        return listTables(session, prefix.getSchema()).stream().collect(toImmutableMap(identity(), schemaTableName -> getRequiredTableMetadata(schemaTableName).getColumns()));
    }

    @Override
    public Optional<ConnectorResolvedIndex> resolveIndex(ConnectorSession session, ConnectorTableHandle tableHandle, Set<ColumnHandle> indexableColumns, Set<ColumnHandle> outputColumns, TupleDomain<ColumnHandle> tupleDomain)
    {
        ThriftTableHandle table = (ThriftTableHandle) tableHandle;
        ThriftTableMetadata tableMetadata = getRequiredTableMetadata(new SchemaTableName(table.getSchemaName(), table.getTableName()));
        if (tableMetadata.containsIndexableColumns(indexableColumns)) {
            return Optional.of(new ConnectorResolvedIndex(new ThriftIndexHandle(tableMetadata.getSchemaTableName(), tupleDomain, session), tupleDomain));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle table, Constraint constraint)
    {
        ThriftTableHandle handle = (ThriftTableHandle) table;

        TupleDomain<ColumnHandle> oldDomain = handle.getConstraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        handle = new ThriftTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                newDomain,
                handle.getDesiredColumns());

        return Optional.of(new ConstraintApplicationResult<>(handle, constraint.getSummary()));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(ConnectorSession session, ConnectorTableHandle table, List<ConnectorExpression> projections, Map<String, ColumnHandle> assignments)
    {
        ThriftTableHandle handle = (ThriftTableHandle) table;

        if (handle.getDesiredColumns().isPresent()) {
            return Optional.empty();
        }

        ImmutableSet.Builder<ColumnHandle> desiredColumns = ImmutableSet.builder();
        ImmutableList.Builder<Assignment> assignmentList = ImmutableList.builder();
        assignments.forEach((name, column) -> {
            desiredColumns.add(column);
            assignmentList.add(new Assignment(name, column, ((ThriftColumnHandle) column).getColumnType()));
        });

        handle = new ThriftTableHandle(
                handle.getSchemaName(),
                handle.getTableName(),
                handle.getConstraint(),
                Optional.of(desiredColumns.build()));

        return Optional.of(new ProjectionApplicationResult<>(handle, projections, assignmentList.build()));
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    private ThriftTableMetadata getRequiredTableMetadata(SchemaTableName schemaTableName)
    {
        Optional<ThriftTableMetadata> table = tableCache.getUnchecked(schemaTableName);
        if (table.isEmpty()) {
            throw new TableNotFoundException(schemaTableName);
        }
        else {
            return table.get();
        }
    }

    // this method makes actual thrift request and should be called only by cache load method
    private Optional<ThriftTableMetadata> getTableMetadataInternal(SchemaTableName schemaTableName)
    {
        requireNonNull(schemaTableName, "schemaTableName is null");
        PrestoThriftNullableTableMetadata thriftTableMetadata = getTableMetadata(schemaTableName);
        if (thriftTableMetadata.getTableMetadata() == null) {
            return Optional.empty();
        }
        ThriftTableMetadata tableMetadata = new ThriftTableMetadata(thriftTableMetadata.getTableMetadata(), typeManager);
        if (!Objects.equals(schemaTableName, tableMetadata.getSchemaTableName())) {
            throw new PrestoException(THRIFT_SERVICE_INVALID_RESPONSE, "Requested and actual table names are different");
        }
        return Optional.of(tableMetadata);
    }

    private PrestoThriftNullableTableMetadata getTableMetadata(SchemaTableName schemaTableName)
    {
        // treat invalid names as not found
        PrestoThriftSchemaTableName name;
        try {
            name = new PrestoThriftSchemaTableName(schemaTableName);
        }
        catch (IllegalArgumentException e) {
            return new PrestoThriftNullableTableMetadata(null);
        }

        try {
            return client.get().getTableMetadata(name);
        }
        catch (PrestoThriftServiceException | TException e) {
            throw toPrestoException(e);
        }
    }
}
