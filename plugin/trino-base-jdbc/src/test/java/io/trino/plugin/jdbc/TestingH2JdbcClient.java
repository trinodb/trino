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
package io.trino.plugin.jdbc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.plugin.jdbc.expression.AggregateFunctionRewriter;
import io.trino.plugin.jdbc.expression.ImplementCountAll;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.AggregateFunction;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.statistics.ColumnStatisticMetadata;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.ComputedStatistics;
import io.trino.spi.statistics.DoubleRange;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import io.trino.spi.statistics.TableStatisticsMetadata;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.sql.Connection;
import java.sql.Types;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.bigintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.booleanColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.dateWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultCharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.defaultVarcharColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.doubleWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.integerWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.realColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.realWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.smallintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.timeColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.timestampColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintColumnMapping;
import static io.trino.plugin.jdbc.StandardColumnMappings.tinyintWriteFunction;
import static io.trino.plugin.jdbc.StandardColumnMappings.varcharWriteFunction;
import static io.trino.plugin.jdbc.TypeHandlingJdbcSessionProperties.getUnsupportedTypeHandling;
import static io.trino.plugin.jdbc.UnsupportedTypeHandling.CONVERT_TO_VARCHAR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static io.trino.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static io.trino.spi.statistics.TableStatisticType.ROW_COUNT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.util.Locale.ENGLISH;

class TestingH2JdbcClient
        extends BaseJdbcClient
{
    private static final JdbcTypeHandle BIGINT_TYPE_HANDLE = new JdbcTypeHandle(Types.BIGINT, Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());

    private final Map<SchemaTableName, JdbcTableStatistics> tableStatistics = new ConcurrentHashMap<>();

    public TestingH2JdbcClient(BaseJdbcConfig config, ConnectionFactory connectionFactory)
    {
        super(config, "\"", connectionFactory);
    }

    @Override
    public boolean supportsAggregationPushdown(ConnectorSession session, JdbcTableHandle table, List<List<ColumnHandle>> groupingSets)
    {
        // GROUP BY with GROUPING SETS is not supported
        return groupingSets.size() == 1;
    }

    @Override
    public Optional<JdbcExpression> implementAggregation(ConnectorSession session, AggregateFunction aggregate, Map<String, ColumnHandle> assignments)
    {
        return new AggregateFunctionRewriter(this::quoted, ImmutableSet.of(new ImplementCountAll(BIGINT_TYPE_HANDLE)))
                .rewrite(session, aggregate, assignments);
    }

    @Override
    public TableStatisticsMetadata getStatisticsCollectionMetadata(ConnectorSession session, ConnectorTableMetadata tableMetadata)
    {
        Set<ColumnStatisticMetadata> columnStatistics = tableMetadata.getColumns().stream()
                .filter(column -> !column.isHidden())
                .map(this::getColumnStatisticMetadata)
                .flatMap(List::stream)
                .collect(toImmutableSet());

        return new TableStatisticsMetadata(columnStatistics, ImmutableSet.of(ROW_COUNT), ImmutableList.of());
    }

    private List<ColumnStatisticMetadata> getColumnStatisticMetadata(ColumnMetadata columnMetadata)
    {
        return Stream.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES)
                .map(columnStatisticType -> new ColumnStatisticMetadata(columnMetadata.getName(), columnStatisticType))
                .collect(toImmutableList());
    }

    @Override
    public void finishStatisticsCollection(ConnectorSession session, JdbcTableHandle tableHandle, Collection<ComputedStatistics> computedStatistics)
    {
        checkArgument(tableHandle.isNamedRelation(), "Expected named relation");
        Map<String, Type> types = tableHandle.getColumns()
                .orElseGet(() -> this.getColumns(session, tableHandle)).stream()
                .collect(toImmutableMap(
                        column -> column.getColumnName().toLowerCase(ENGLISH),
                        column -> column.getColumnType()));
        Optional<JdbcTableStatistics> jdbcTableStatistics = computedStatistics.stream()
                .map(statistics -> JdbcTableStatistics.from(statistics, types))
                .reduce(JdbcTableStatistics::merge);
        if (jdbcTableStatistics.isPresent()) {
            tableStatistics.put(tableHandle.getRequiredNamedRelation().getSchemaTableName(), jdbcTableStatistics.get());
        }
    }

    @Override
    public TableStatistics getTableStatistics(ConnectorSession session, JdbcTableHandle handle, TupleDomain<ColumnHandle> tupleDomain)
    {
        if (!handle.isNamedRelation()) {
            return TableStatistics.empty();
        }
        JdbcTableStatistics jdbcTableStatistics = this.tableStatistics.get(handle.getRequiredNamedRelation().getSchemaTableName());
        if (jdbcTableStatistics == null) {
            return TableStatistics.empty();
        }

        Map<String, JdbcColumnHandle> columnHandles = handle.getColumns()
                .orElseGet(() -> this.getColumns(session, handle)).stream()
                .collect(toImmutableMap(
                        column -> column.getColumnName().toLowerCase(ENGLISH),
                        Function.identity()));

        TableStatistics.Builder tableStatistics = TableStatistics.builder();
        Estimate rowsCount = jdbcTableStatistics.getRowsCount();
        tableStatistics.setRowCount(rowsCount);
        if (rowsCount.getValue() <= 0) {
            return tableStatistics.build();
        }
        jdbcTableStatistics.getColumnStatistics().entrySet()
                .forEach(entry -> {
                    String columnName = entry.getKey();
                    JdbcColumnStatistics jdbcColumnStatistics = entry.getValue();
                    ColumnStatistics.Builder columnStatistics = ColumnStatistics.builder();
                    columnStatistics.setDistinctValuesCount(jdbcColumnStatistics.getDistinctValuesCount());
                    columnStatistics.setNullsFraction(Estimate.of((rowsCount.getValue() - jdbcColumnStatistics.getNonNullsCount().getValue()) / rowsCount.getValue()));
                    columnStatistics.setRange(jdbcColumnStatistics.getIntegerStatistics().map(integerStatistics -> {
                        OptionalLong min = integerStatistics.getMin();
                        OptionalLong max = integerStatistics.getMax();
                        return new DoubleRange(
                                min.isPresent() ? min.getAsLong() : Double.NEGATIVE_INFINITY,
                                max.isPresent() ? max.getAsLong() : Double.POSITIVE_INFINITY);
                    }));
                    tableStatistics.setColumnStatistics(columnHandles.get(columnName), columnStatistics.build());
                });
        return tableStatistics.build();
    }

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle)
    {
        Optional<ColumnMapping> mapping = getForcedMappingToVarchar(typeHandle);
        if (mapping.isPresent()) {
            return mapping;
        }

        switch (typeHandle.getJdbcType()) {
            case Types.BOOLEAN:
                return Optional.of(booleanColumnMapping());

            case Types.TINYINT:
                return Optional.of(tinyintColumnMapping());

            case Types.SMALLINT:
                return Optional.of(smallintColumnMapping());

            case Types.INTEGER:
                return Optional.of(integerColumnMapping());

            case Types.BIGINT:
                return Optional.of(bigintColumnMapping());

            case Types.REAL:
                return Optional.of(realColumnMapping());

            case Types.DOUBLE:
                return Optional.of(doubleColumnMapping());

            case Types.CHAR:
                return Optional.of(defaultCharColumnMapping(typeHandle.getRequiredColumnSize(), true));

            case Types.VARCHAR:
                return Optional.of(defaultVarcharColumnMapping(typeHandle.getRequiredColumnSize(), true));

            case Types.DATE:
                return Optional.of(dateColumnMapping());

            case Types.TIME:
                return Optional.of(timeColumnMapping(TIME_MILLIS));

            case Types.TIMESTAMP:
                return Optional.of(timestampColumnMapping(TIMESTAMP_MILLIS));
        }

        if (getUnsupportedTypeHandling(session) == CONVERT_TO_VARCHAR) {
            return mapToUnboundedVarchar(typeHandle);
        }

        return Optional.empty();
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type)
    {
        if (type == TINYINT) {
            return WriteMapping.longMapping("tinyint", tinyintWriteFunction());
        }
        if (type == SMALLINT) {
            return WriteMapping.longMapping("smallint", smallintWriteFunction());
        }
        if (type == INTEGER) {
            return WriteMapping.longMapping("integer", integerWriteFunction());
        }
        if (type == BIGINT) {
            return WriteMapping.longMapping("bigint", bigintWriteFunction());
        }
        if (type == REAL) {
            return WriteMapping.longMapping("float", realWriteFunction());
        }
        if (type == DOUBLE) {
            return WriteMapping.doubleMapping("double precision", doubleWriteFunction());
        }

        if (type instanceof VarcharType) {
            VarcharType varcharType = (VarcharType) type;
            String dataType = varcharType.isUnbounded() ? "varchar" : "varchar(" + varcharType.getBoundedLength() + ")";
            return WriteMapping.sliceMapping(dataType, varcharWriteFunction());
        }

        if (type == DATE) {
            return WriteMapping.longMapping("date", dateWriteFunction());
        }

        throw new TrinoException(NOT_SUPPORTED, "Unsupported column type: " + type.getDisplayName());
    }

    @Override
    protected void renameTable(ConnectorSession session, String catalogName, String schemaName, String tableName, SchemaTableName newTable)
    {
        if (!schemaName.equalsIgnoreCase(newTable.getSchemaName())) {
            throw new TrinoException(NOT_SUPPORTED, "This connector does not support renaming tables across schemas");
        }
        super.renameTable(session, catalogName, schemaName, tableName, newTable);
    }
}
