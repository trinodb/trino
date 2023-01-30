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
package io.trino.plugin.redshift;

import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.statistics.ColumnStatistics;
import io.trino.spi.statistics.Estimate;
import io.trino.spi.statistics.TableStatistics;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

public class RedshiftTableStatisticsReader
{
    private final ConnectionFactory connectionFactory;

    public RedshiftTableStatisticsReader(ConnectionFactory connectionFactory)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
    }

    public TableStatistics readTableStatistics(ConnectorSession session, JdbcTableHandle table, Supplier<List<JdbcColumnHandle>> columnSupplier)
            throws SQLException
    {
        checkArgument(table.isNamedRelation(), "Relation is not a table: %s", table);

        try (Connection connection = connectionFactory.openConnection(session);
                Handle handle = Jdbi.open(connection)) {
            StatisticsDao statisticsDao = new StatisticsDao(handle);

            RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
            Optional<Long> optionalRowCount = readRowCountTableStat(statisticsDao, table);
            if (optionalRowCount.isEmpty()) {
                // Table not found
                return TableStatistics.empty();
            }
            long rowCount = optionalRowCount.get();

            TableStatistics.Builder tableStatistics = TableStatistics.builder()
                    .setRowCount(Estimate.of(rowCount));

            if (rowCount == 0) {
                return tableStatistics.build();
            }

            Map<String, ColumnStatisticsResult> columnStatistics = statisticsDao.getColumnStatistics(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName()).stream()
                    .collect(toImmutableMap(ColumnStatisticsResult::columnName, identity()));

            for (JdbcColumnHandle column : columnSupplier.get()) {
                ColumnStatisticsResult result = columnStatistics.get(column.getColumnName());
                if (result == null) {
                    continue;
                }

                ColumnStatistics statistics = ColumnStatistics.builder()
                        .setNullsFraction(result.nullsFraction()
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDistinctValuesCount(result.distinctValuesIndicator()
                                .map(distinctValuesIndicator -> {
                                    // If the distinct value count is an estimate Redshift uses "the negative of the number of distinct values divided by the number of rows
                                    // For example, -1 indicates a unique column in which the number of distinct values is the same as the number of rows."
                                    // https://www.postgresql.org/docs/9.3/view-pg-stats.html
                                    if (distinctValuesIndicator < 0.0) {
                                        return Math.min(-distinctValuesIndicator * rowCount, rowCount);
                                    }
                                    return distinctValuesIndicator;
                                })
                                .map(Estimate::of)
                                .orElseGet(Estimate::unknown))
                        .setDataSize(result.averageColumnLength()
                                .flatMap(averageColumnLength ->
                                        result.nullsFraction()
                                                .map(nullsFraction -> 1.0 * averageColumnLength * rowCount * (1 - nullsFraction))
                                                .map(Estimate::of))
                                .orElseGet(Estimate::unknown))
                        .build();

                tableStatistics.setColumnStatistics(column, statistics);
            }

            return tableStatistics.build();
        }
    }

    private static Optional<Long> readRowCountTableStat(StatisticsDao statisticsDao, JdbcTableHandle table)
    {
        RemoteTableName remoteTableName = table.getRequiredNamedRelation().getRemoteTableName();
        Optional<Long> rowCount = statisticsDao.getRowCountFromPgClass(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName());
        if (rowCount.isEmpty()) {
            // Table not found
            return Optional.empty();
        }

        if (rowCount.get() == 0) {
            // `pg_class.reltuples = 0` may mean an empty table or a recently populated table (CTAS, LOAD or INSERT)
            // The `pg_stat_all_tables` view can be way off, so we use it only as a fallback
            rowCount = statisticsDao.getRowCountFromPgStat(remoteTableName.getSchemaName().orElse(null), remoteTableName.getTableName());
        }

        return rowCount;
    }

    private static class StatisticsDao
    {
        private final Handle handle;

        public StatisticsDao(Handle handle)
        {
            this.handle = requireNonNull(handle, "handle is null");
        }

        Optional<Long> getRowCountFromPgClass(String schema, String tableName)
        {
            return handle.createQuery("SELECT reltuples FROM pg_class WHERE relnamespace = (SELECT oid FROM pg_namespace WHERE nspname = :schema) AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        Optional<Long> getRowCountFromPgStat(String schema, String tableName)
        {
            // Redshift does not have the Postgres `n_live_tup`, so estimate from `inserts - deletes`
            return handle.createQuery("SELECT n_tup_ins - n_tup_del FROM pg_stat_all_tables WHERE schemaname = :schema AND relname = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .mapTo(Long.class)
                    .findOne();
        }

        List<ColumnStatisticsResult> getColumnStatistics(String schema, String tableName)
        {
            return handle.createQuery("SELECT attname, null_frac, n_distinct, avg_width FROM pg_stats WHERE schemaname = :schema AND tablename = :table_name")
                    .bind("schema", schema)
                    .bind("table_name", tableName)
                    .map((rs, ctx) ->
                            new ColumnStatisticsResult(
                                    requireNonNull(rs.getString("attname"), "attname is null"),
                                    Optional.of(rs.getFloat("null_frac")),
                                    Optional.of(rs.getFloat("n_distinct")),
                                    Optional.of(rs.getInt("avg_width"))))
                    .list();
        }
    }

    private record ColumnStatisticsResult(String columnName, Optional<Float> nullsFraction, Optional<Float> distinctValuesIndicator, Optional<Integer> averageColumnLength)
    {
        ColumnStatisticsResult
        {
            requireNonNull(columnName, "columnName is null");
            requireNonNull(nullsFraction, "nullsFraction is null");
            requireNonNull(distinctValuesIndicator, "distinctValuesIndicator is null");
            requireNonNull(averageColumnLength, "averageColumnLength is null");
        }
    }
}
