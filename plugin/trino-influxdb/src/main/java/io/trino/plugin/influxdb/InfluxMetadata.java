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

package io.trino.plugin.influxdb;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.Assignment;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.ConnectorTableVersion;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.ConstraintApplicationResult;
import io.trino.spi.connector.LimitApplicationResult;
import io.trino.spi.connector.ProjectionApplicationResult;
import io.trino.spi.connector.RelationColumnsMetadata;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.SchemaTablePrefix;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.EquatableValueSet;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.SortedRangeSet;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.TrinoPrincipal;
import io.trino.spi.type.TimestampType;
import jakarta.annotation.Nullable;
import org.influxdb.dto.QueryResult;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.UnaryOperator;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.influxdb.TypeUtils.isPushdownSupportedType;
import static io.trino.spi.StandardErrorCode.CATALOG_STORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.SCHEMA_NOT_EMPTY;
import static io.trino.spi.connector.RelationColumnsMetadata.forTable;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class InfluxMetadata
        implements ConnectorMetadata
{
    private final InfluxClient client;

    @Inject
    public InfluxMetadata(InfluxClient client)
    {
        this.client = requireNonNull(client, "client is null");
    }

    @Override
    public List<String> listSchemaNames(ConnectorSession session)
    {
        return ImmutableList.copyOf(client.listSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> optionalSchemaName)
    {
        Set<String> schemaNames = optionalSchemaName.map(ImmutableSet::of)
                .orElseGet(() -> ImmutableSet.copyOf(client.listSchemaNames()));

        return schemaNames.stream()
                .flatMap(schemaName -> client.getSchemaTableNames(schemaName).stream())
                .collect(toImmutableList());
    }

    @Nullable
    @Override
    public ConnectorTableHandle getTableHandle(
            ConnectorSession session,
            SchemaTableName schemaTableName,
            Optional<ConnectorTableVersion> startVersion,
            Optional<ConnectorTableVersion> endVersion)
    {
        if (startVersion.isPresent() || endVersion.isPresent()) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support versioned tables");
        }

        return getTableHandle(session, schemaTableName);
    }

    @Nullable
    public ConnectorTableHandle getTableHandle(ConnectorSession session, SchemaTableName schemaTableName)
    {
        return client.findTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName())
                .map(ignored -> InfluxTableHandle.of(schemaTableName.getSchemaName(), schemaTableName.getTableName())).orElse(null);
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        return getTableMetadata(((InfluxTableHandle) tableHandle).toSchemaTableName())
                .orElseThrow(() -> new RuntimeException("The table handle is invalid " + tableHandle));
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        InfluxTableHandle influxTableHandle = (InfluxTableHandle) tableHandle;

        InfluxTableHandle table = client.findTableHandle(influxTableHandle.schemaName(), influxTableHandle.tableName())
                .orElseThrow(() -> new TableNotFoundException(influxTableHandle.toSchemaTableName()));

        ImmutableMap.Builder<String, ColumnHandle> columnHandles =
                ImmutableMap.builderWithExpectedSize(table.columns().size());
        for (InfluxColumnHandle column : table.columns()) {
            columnHandles.put(column.columnName(), column);
        }
        return columnHandles.buildOrThrow();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        InfluxColumnHandle influxColumnHandle = (InfluxColumnHandle) columnHandle;
        return new ColumnMetadata(influxColumnHandle.columnName(), influxColumnHandle.columnType());
    }

    @Override
    public Iterator<RelationColumnsMetadata> streamRelationColumns(ConnectorSession session, Optional<String> schemaName, UnaryOperator<Set<SchemaTableName>> relationFilter)
    {
        Map<SchemaTableName, RelationColumnsMetadata> relationColumns = new HashMap<>();

        SchemaTablePrefix prefix = schemaName.map(SchemaTablePrefix::new)
                .orElseGet(SchemaTablePrefix::new);

        for (SchemaTableName tableName : listTables(session, prefix.getSchema())) {
            Optional<ConnectorTableMetadata> tableMetadata = getTableMetadata(tableName);
            if (tableMetadata.isPresent()) {
                relationColumns.put(tableName, forTable(tableName, tableMetadata.get().getColumns()));
            }
        }

        return relationFilter.apply(relationColumns.keySet()).stream()
                .map(relationColumns::get)
                .iterator();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(
            ConnectorSession session,
            ConnectorTableHandle handle,
            long limit)
    {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        // InfluxQL Limit 0 is equivalent to setting no limit
        if (limit == 0) {
            return Optional.empty();
        }
        // InfluxQL doesn't support limit number greater than integer max
        if (limit > Integer.MAX_VALUE) {
            return Optional.empty();
        }
        if (tableHandle.limit().isPresent() && tableHandle.limit().getAsInt() <= limit) {
            return Optional.empty();
        }

        return Optional.of(new LimitApplicationResult<>(
                tableHandle.withLimit(OptionalInt.of(toIntExact(limit))),
                true,
                false));
    }

    @Override
    public void dropTable(ConnectorSession session, ConnectorTableHandle handle)
    {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        if (client.findTableHandle(tableHandle.schemaName(), tableHandle.tableName()).isEmpty()) {
            throw new TableNotFoundException(tableHandle.toSchemaTableName());
        }

        client.dropTable(tableHandle.schemaName(), tableHandle.tableName());
    }

    @Override
    public void dropSchema(ConnectorSession session, String schemaName, boolean cascade)
    {
        if (!client.listSchemaNames().contains(schemaName)) {
            throw new SchemaNotFoundException(schemaName);
        }

        if (!cascade) {
            boolean isEmpty = listTables(session, Optional.of(schemaName)).isEmpty();
            if (!isEmpty) {
                throw new TrinoException(SCHEMA_NOT_EMPTY, "Cannot drop non-empty schema '%s'".formatted(schemaName));
            }
        }
        client.dropSchema(schemaName);
    }

    @Override
    public void createSchema(ConnectorSession session, String schemaName, Map<String, Object> properties, TrinoPrincipal owner)
    {
        QueryResult result = client.createSchema(schemaName);
        if (!result.getResults().getFirst().hasError()) {
            return;
        }
        throw new TrinoException(CATALOG_STORE_ERROR, "Failed to create schema: " + result.getResults().getFirst().getError());
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(
            ConnectorSession session,
            ConnectorTableHandle handle,
            Constraint constraint)
    {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        TupleDomain<ColumnHandle> oldDomain = tableHandle.constraint();
        TupleDomain<ColumnHandle> newDomain = oldDomain.intersect(constraint.getSummary());
        TupleDomain<ColumnHandle> remainingFilter;
        if (newDomain.isNone()) {
            remainingFilter = TupleDomain.all();
        }
        else {
            Map<ColumnHandle, Domain> domains = newDomain.getDomains().orElseThrow();
            Map<ColumnHandle, Domain> supported = new HashMap<>();
            Map<ColumnHandle, Domain> unsupported = new HashMap<>();
            domains.forEach((key, domain) -> {
                if (isPushdownSupportedType(((InfluxColumnHandle) key).columnType())
                        && isPushdownSupportedDomain(domain)) {
                    supported.put(key, domain);
                }
                else {
                    unsupported.put(key, domain);
                }
            });
            newDomain = TupleDomain.withColumnDomains(supported);
            remainingFilter = TupleDomain.withColumnDomains(unsupported);
        }

        if (oldDomain.equals(newDomain)) {
            return Optional.empty();
        }

        return Optional.of(new ConstraintApplicationResult<>(
                tableHandle.withConstraint(newDomain),
                remainingFilter,
                constraint.getExpression(),
                false));
    }

    @Override
    public Optional<ProjectionApplicationResult<ConnectorTableHandle>> applyProjection(
            ConnectorSession session,
            ConnectorTableHandle handle,
            List<ConnectorExpression> projections,
            Map<String, ColumnHandle> assignments)
    {
        InfluxTableHandle tableHandle = (InfluxTableHandle) handle;
        List<ColumnHandle> oldProjections = ((InfluxTableHandle) handle).projections();
        List<ColumnHandle> newProjections = ImmutableList.copyOf(assignments.values());
        if (oldProjections.equals(newProjections)) {
            return Optional.empty();
        }

        List<Assignment> assignmentsList = assignments.entrySet().stream()
                .map(assignment -> new Assignment(
                        assignment.getKey(),
                        assignment.getValue(),
                        ((InfluxColumnHandle) assignment.getValue()).columnType()))
                .collect(toImmutableList());
        return Optional.of(new ProjectionApplicationResult<>(
                tableHandle.withProjections(newProjections),
                projections,
                assignmentsList,
                false));
    }

    private Optional<ConnectorTableMetadata> getTableMetadata(SchemaTableName schemaTableName)
    {
        Optional<InfluxTableHandle> tableHandle = client.findTableHandle(schemaTableName.getSchemaName(), schemaTableName.getTableName());

        return tableHandle.map(table -> {
            ImmutableList.Builder<ColumnMetadata> columnMetadataBuilder =
                    ImmutableList.builderWithExpectedSize(table.columns().size());
            List<InfluxColumnHandle> columns = table.columns();
            for (InfluxColumnHandle column : columns) {
                columnMetadataBuilder.add(new ColumnMetadata(column.columnName(), column.columnType()));
            }
            return new ConnectorTableMetadata(schemaTableName, columnMetadataBuilder.build());
        });
    }

    private static boolean isPushdownSupportedDomain(Domain domain)
    {
        if (domain.getValues() instanceof SortedRangeSet rangeSet) {
            if (rangeSet.getOrderedRanges().isEmpty()) {
                return false;
            }
            List<Range> ranges = rangeSet.getOrderedRanges();
            return isRangeSupportsTimestamp(ranges) ||
                    isRangeSupportsBoolean(ranges) ||
                    isRangeSupportsNumber(ranges) ||
                    isRangeSupportsVarchar(ranges);
        }
        if (domain.getValues() instanceof EquatableValueSet valueSet) {
            return !valueSet.getDiscreteSet().isEmpty();
        }
        return false;
    }

    private static boolean isRangeSupportsVarchar(List<Range> ranges)
    {
        return ranges.stream().allMatch(range -> range.getType() == VARCHAR) &&
                ranges.stream().anyMatch(Range::isSingleValue);
    }

    private static boolean isRangeSupportsBoolean(List<Range> ranges)
    {
        return ranges.stream().allMatch(range -> range.getType() == BOOLEAN) &&
                ranges.stream().anyMatch(Range::isSingleValue);
    }

    private static boolean isRangeSupportsNumber(List<Range> ranges)
    {
        return ranges.stream().allMatch(range -> range.getType() == DOUBLE || range.getType() == BIGINT);
    }

    private static boolean isRangeSupportsTimestamp(List<Range> ranges)
    {
        return ranges.stream().allMatch(range -> range.getType() instanceof TimestampType);
    }
}
