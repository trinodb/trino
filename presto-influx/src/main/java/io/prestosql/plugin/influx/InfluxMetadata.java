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

package io.prestosql.plugin.influx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ColumnMetadata;
import io.prestosql.spi.connector.ConnectorMetadata;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTableMetadata;
import io.prestosql.spi.connector.ConnectorTableProperties;
import io.prestosql.spi.connector.Constraint;
import io.prestosql.spi.connector.ConstraintApplicationResult;
import io.prestosql.spi.connector.LimitApplicationResult;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.connector.SchemaTablePrefix;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.EquatableValueSet;
import io.prestosql.spi.predicate.Range;
import io.prestosql.spi.predicate.SortedRangeSet;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.predicate.ValueSet;
import io.prestosql.spi.type.DateTimeEncoding;

import javax.inject.Inject;

import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

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
        return ImmutableList.copyOf(client.getSchemaNames());
    }

    @Override
    public List<SchemaTableName> listTables(ConnectorSession session, Optional<String> schemaName)
    {
        Collection<String> schemaNames;
        if (schemaName.isPresent()) {
            if (client.getSchemaNames().contains(schemaName.get())) {
                schemaNames = Collections.singletonList(schemaName.get());
            }
            else {
                return Collections.emptyList();
            }
        }
        else {
            schemaNames = client.getSchemaNames();
        }
        // in Influx, all measurements can exist in all retention policies,
        // (and all tickets asking for a way to know which measurements are actually
        // used in which retention policy are closed as wont-fix)
        ImmutableList.Builder<SchemaTableName> builder = ImmutableList.builder();
        for (String tableName : client.getTableNames()) {
            for (String matchingSchemaName : schemaNames) {
                if (client.tableExistsInSchema(matchingSchemaName, tableName)) {
                    builder.add(new SchemaTableName(matchingSchemaName, tableName));
                }
            }
        }
        return builder.build();
    }

    @Override
    public InfluxTableHandle getTableHandle(ConnectorSession session, SchemaTableName tableName)
    {
        String retentionPolicy = client.getRetentionPolicy(tableName.getSchemaName());
        String measurement = client.getMeasurement(tableName.getTableName());
        if (retentionPolicy != null && measurement != null) {
            return new InfluxTableHandle(retentionPolicy, measurement);
        }
        return null;
    }

    @Override
    public Map<SchemaTableName, List<ColumnMetadata>> listTableColumns(ConnectorSession session, SchemaTablePrefix prefix)
    {
        requireNonNull(prefix, "prefix is null");
        ImmutableMap.Builder<SchemaTableName, List<ColumnMetadata>> result = ImmutableMap.builder();
        Collection<String> schemaNames = client.getSchemaNames();
        Collection<String> tableNames = client.getTableNames();
        for (String schemaName : schemaNames) {
            for (String tableName : tableNames) {
                SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
                if (prefix.matches(schemaTableName)) {
                    List<InfluxColumn> columns = client.getColumns(schemaName, tableName);
                    if (!columns.isEmpty()) {
                        result.put(schemaTableName, ImmutableList.copyOf(columns));
                    }
                }
            }
        }
        return result.build();
    }

    @Override
    public ConnectorTableMetadata getTableMetadata(ConnectorSession session, ConnectorTableHandle table)
    {
        InfluxTableHandle influxTable = (InfluxTableHandle) table;
        ImmutableList.Builder<ColumnMetadata> columns = new ImmutableList.Builder<>();
        for (InfluxColumn column : client.getColumns(influxTable.getSchemaName(), influxTable.getTableName())) {
            columns.add(new InfluxColumnHandle(influxTable.getRetentionPolicy(), influxTable.getMeasurement(), column));
        }
        return new ConnectorTableMetadata(influxTable, columns.build());
    }

    @Override
    public Map<String, ColumnHandle> getColumnHandles(ConnectorSession session, ConnectorTableHandle tableHandle)
    {
        ImmutableMap.Builder<String, ColumnHandle> handles = new ImmutableMap.Builder<>();
        for (ColumnMetadata column : getTableMetadata(session, tableHandle).getColumns()) {
            handles.put(column.getName(), (InfluxColumnHandle) column);
        }
        return handles.build();
    }

    @Override
    public ColumnMetadata getColumnMetadata(ConnectorSession session, ConnectorTableHandle tableHandle, ColumnHandle columnHandle)
    {
        return (InfluxColumnHandle) columnHandle;
    }

    @Override
    public boolean usesLegacyTableLayouts()
    {
        return false;
    }

    @Override
    public ConnectorTableProperties getTableProperties(ConnectorSession session, ConnectorTableHandle table)
    {
        return new ConnectorTableProperties();
    }

    @Override
    public Optional<LimitApplicationResult<ConnectorTableHandle>> applyLimit(ConnectorSession session, ConnectorTableHandle handle, long limit)
    {
        InfluxTableHandle table = (InfluxTableHandle) handle;
        return Optional.of(new LimitApplicationResult<>(new InfluxTableHandle(
                table.getRetentionPolicy(),
                table.getMeasurement(),
                table.getWhere(),
                limit), true));
    }

    @Override
    public Optional<ConstraintApplicationResult<ConnectorTableHandle>> applyFilter(ConnectorSession session, ConnectorTableHandle handle, Constraint constraint)
    {
        boolean all = true;
        InfluxQL where = new InfluxQL();
        for (Map.Entry<ColumnHandle, Domain> predicate : constraint.getSummary().getDomains().orElse(Collections.emptyMap()).entrySet()) {
            int startPos = where.getPos();
            InfluxColumnHandle column = (InfluxColumnHandle) predicate.getKey();
            ValueSet values = predicate.getValue().getValues();
            AtomicBoolean ok = new AtomicBoolean(true);  // can we handle this column?
            Consumer<String> fail = error -> {
                client.logger.debug("unhandled " + error + " " + column + ": " + values.toString(session));
                ok.set(false);
            };
            if (values instanceof SortedRangeSet) {
                boolean first = true;
                ranges:
                for (Range range : values.getRanges().getOrderedRanges()) {
                    if (!range.isSingleValue() && !range.getLow().getValueBlock().isPresent() && !range.getHigh().getValueBlock().isPresent()) {
                        // can't do an IS NULL
                        fail.accept("range");
                        break;
                    }
                    where.append(first ? where.isEmpty() ? "WHERE ((" : " AND ((" : ") OR (");
                    if (range.isSingleValue()) {
                        Object value = range.getSingleValue();
                        if (column.getKind() == InfluxColumn.Kind.TIME) {
                            if (value instanceof Long) {
                                value = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc((Long) value)).toString();
                            }
                            else {
                                fail.accept("time");
                                break;
                            }
                        }
                        where.add(column).append(" = ").add(value);
                    }
                    else {
                        boolean hasLow = false;
                        if (range.getLow().getValueBlock().isPresent()) {
                            final String low;
                            switch (range.getLow().getBound()) {
                                case EXACTLY:
                                    low = " >= ";
                                    break;
                                case ABOVE:
                                    low = " > ";
                                    break;
                                default:
                                    fail.accept("low bound");
                                    break ranges;
                            }
                            Object value = range.getLow().getValue();
                            if (column.getKind() == InfluxColumn.Kind.TIME) {
                                if (value instanceof Long) {
                                    value = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc((Long) value)).toString();
                                }
                                else {
                                    fail.accept("time low bound");
                                    break;
                                }
                            }
                            else if (!(value instanceof Number)) {
                                fail.accept("tag comparision low bound");
                                break;
                            }
                            where.add(column).append(low).add(value);
                            hasLow = true;
                        }
                        if (range.getHigh().getValueBlock().isPresent()) {
                            final String high;
                            switch (range.getHigh().getBound()) {
                                case EXACTLY:
                                    high = " <= ";
                                    break;
                                case BELOW:
                                    high = " < ";
                                    break;
                                default:
                                    fail.accept("high bound");
                                    break ranges;
                            }
                            if (hasLow) {
                                where.append(" AND ");
                            }
                            Object value = range.getHigh().getValue();
                            if (column.getKind() == InfluxColumn.Kind.TIME) {
                                if (value instanceof Long) {
                                    value = Instant.ofEpochMilli(DateTimeEncoding.unpackMillisUtc((Long) value)).toString();
                                }
                                else {
                                    fail.accept("time high bound");
                                    break;
                                }
                            }
                            else if (!(value instanceof Number)) {
                                fail.accept("tag comparison high bound");
                                break;
                            }
                            where.add(column).append(high).add(value);
                        }
                    }
                    first = false;
                }
                if (ok.get() && first) {
                    fail.accept("SortedRangeSet");
                }
                else {
                    where.append("))");
                }
            }
            else if (values instanceof EquatableValueSet) {
                boolean first = true;
                for (Object value : values.getDiscreteValues().getValues()) {
                    where.append(first ? where.isEmpty() ? "WHERE (" : " AND (" : " OR ")
                            .add(column).append(" = ").add(value);
                    first = false;
                }
                if (first) {
                    fail.accept("EquatableValueSet");
                }
                else {
                    where.append(')');
                }
            }
            else {
                fail.accept("predicate");
            }
            if (!ok.get()) {
                // undo everything we did add to the where-clause
                where.truncate(startPos);
                // and tell Presto we couldn't handle all the filtering
                all = false;
            }
        }
        client.logger.debug("applyFilter(" + handle + ", " + constraint.getSummary().toString(session) + ") = " + all + ", " + where);
        InfluxTableHandle table = (InfluxTableHandle) handle;
        return Optional.of(new ConstraintApplicationResult<>(new InfluxTableHandle(
                table.getRetentionPolicy(),
                table.getMeasurement(),
                where,
                table.getLimit()), all ? TupleDomain.all() : constraint.getSummary()));
    }
}
