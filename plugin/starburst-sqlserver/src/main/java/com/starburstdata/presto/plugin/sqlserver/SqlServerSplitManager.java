/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.sqlserver;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.starburstdata.presto.license.LicenseManager;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.ConnectionFactory;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcSplit;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.RemoteTableName;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import io.trino.spi.predicate.TupleDomain;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.core.JdbiException;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Lists.partition;
import static com.google.common.math.IntMath.divide;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.getConnectionsCount;
import static com.starburstdata.presto.plugin.sqlserver.StarburstSqlServerSessionProperties.hasParallelism;
import static io.trino.plugin.jdbc.DynamicFilteringJdbcSplitSource.isEligibleForDynamicFilter;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static java.math.RoundingMode.CEILING;
import static java.util.Objects.requireNonNull;
import static java.util.stream.IntStream.rangeClosed;

public class SqlServerSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(SqlServerSplitManager.class);
    private final ConnectionFactory connectionFactory;

    @Inject
    public SqlServerSplitManager(
            ConnectionFactory connectionFactory,
            StarburstSqlServerConfig starburstSqlServerConfig,
            LicenseManager licenseManager)
    {
        this.connectionFactory = requireNonNull(connectionFactory, "connectionFactory is null");
        if (starburstSqlServerConfig.getConnectionsCount() > 1) {
            licenseManager.checkLicense();
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        return new FixedSplitSource(listSplits(
                session,
                (JdbcTableHandle) table,
                getConnectionsCount(session),
                isEligibleForDynamicFilter((JdbcTableHandle) table)
                        ? dynamicFilter.getCurrentPredicate().transformKeys(JdbcColumnHandle.class::cast)
                        : TupleDomain.all()));
    }

    private List<JdbcSplit> listSplits(
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            int connectionsCount,
            TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        if (!hasParallelism(session) || !tableHandle.isNamedRelation()) {
            return ImmutableList.of(new JdbcSplit(Optional.empty(), dynamicFilter));
        }

        return listPartitionsAndBuildSplitsWithRetries(session, tableHandle, connectionsCount, dynamicFilter);
    }

    private List<JdbcSplit> listPartitionsAndBuildSplits(ConnectorSession session, JdbcTableHandle tableHandle, int maxSplits, TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        try (Handle handle = Jdbi.open(() -> connectionFactory.openConnection(session))) {
            RemoteTableName remoteTableName = tableHandle.getRequiredNamedRelation().getRemoteTableName();

            return handle.createQuery("""
                            SELECT
                               sys_columns.name AS column_name,
                               sys_functions.name AS function_name,
                               sys_functions.fanout AS partition_fanout
                            FROM sys.tables AS sys_tables
                            JOIN sys.indexes AS sys_indexes
                               ON sys_tables.object_id = sys_indexes.object_id
                            JOIN sys.partition_schemes AS sys_schemas
                               ON sys_schemas.data_space_id = sys_indexes.data_space_id
                            JOIN sys.partition_functions AS sys_functions
                               ON sys_functions.function_id = sys_schemas.function_id
                            JOIN sys.index_columns AS sys_index_columns
                               ON sys_index_columns.object_id = sys_indexes.object_id
                               AND sys_index_columns.index_id = sys_indexes.index_id
                               AND sys_index_columns.partition_ordinal >= 1
                            JOIN sys.columns AS sys_columns
                               ON sys_tables.object_id = sys_columns.object_id
                               AND sys_index_columns.column_id = sys_columns.column_id
                            WHERE sys_tables.name = :name
                            """)
                    // sys_index_columns.partition_ordinal >= 1 is a partitioning column?
                    // Ordinal within set of partitioning columns.
                    .bind("name", remoteTableName.getTableName())
                    .map((rs, ctx) -> PartitionSplitBuilder.builder()
                            .withColumnName(rs.getString("column_name"))
                            .withFunctionName(rs.getString("function_name"))
                            .withPartitionFanout(rs.getInt("partition_fanout"))
                            .withDynamicFilter(dynamicFilter)
                            .withMaxSplits(maxSplits)
                            .build())
                    .findOne()
                    .orElse(ImmutableList.of(new JdbcSplit(Optional.empty(), dynamicFilter)));
        }
        catch (JdbiException e) {
            throw new TrinoException(JDBC_ERROR, e);
        }
    }

    private List<JdbcSplit> listPartitionsAndBuildSplitsWithRetries(ConnectorSession session, JdbcTableHandle tableHandle, int maxSplits, TupleDomain<JdbcColumnHandle> dynamicFilter)
    {
        // DDL operations can take out locks against system tables causing the `getTableDataCompression` query to deadlock
        final int maxAttemptCount = 3;
        RetryPolicy<List<JdbcSplit>> retryPolicy = RetryPolicy.<List<JdbcSplit>>builder()
                .withMaxAttempts(maxAttemptCount)
                .handleIf(throwable ->
                {
                    final int deadlockErrorCode = 1205;
                    Throwable rootCause = Throwables.getRootCause(throwable);
                    return rootCause instanceof SQLServerException &&
                            ((SQLServerException) rootCause).getSQLServerError().getErrorNumber() == deadlockErrorCode;
                })
                .onFailedAttempt(event -> log.warn(event.getLastException(), "Attempt %d of %d: error when listing partitions and creating splits for '%s'", event.getAttemptCount(), maxAttemptCount, tableHandle))
                .build();

        return Failsafe
                .with(retryPolicy)
                .get(() -> listPartitionsAndBuildSplits(session, tableHandle, maxSplits, dynamicFilter));
    }

    private static class PartitionSplitBuilder
    {
        private static final String PARTITION_PREDICATE_PATTERN = "$PARTITION.%s(%s) BETWEEN %s AND %s";
        private String columnName;
        private String functionName;
        private Integer partitionFanout;
        private Integer maxSplits;

        private TupleDomain<JdbcColumnHandle> dynamicFilter;

        private PartitionSplitBuilder() {}

        public PartitionSplitBuilder withColumnName(String columnName)
        {
            this.columnName = columnName;
            return this;
        }

        public PartitionSplitBuilder withFunctionName(String functionName)
        {
            this.functionName = functionName;
            return this;
        }

        public PartitionSplitBuilder withPartitionFanout(int partitionFanout)
        {
            this.partitionFanout = partitionFanout;
            return this;
        }

        public PartitionSplitBuilder withMaxSplits(int maxSplits)
        {
            this.maxSplits = maxSplits;
            return this;
        }

        public PartitionSplitBuilder withDynamicFilter(TupleDomain<JdbcColumnHandle> dynamicFilter)
        {
            this.dynamicFilter = dynamicFilter;
            return this;
        }

        public List<JdbcSplit> build()
        {
            requireNonNull(functionName, "functionName is null");
            requireNonNull(columnName, "columnName is null");
            requireNonNull(dynamicFilter, "dynamicFilter is null");
            requireNonNull(partitionFanout, "partitionFanout is null");
            requireNonNull(maxSplits, "maxSplits is null");

            // In case we have more partitions than maxSplits, we should aggregate them into aggregated splits
            // with common predicate, containing multiple partitions using BETWEEN clause
            return partition(rangeClosed(1, partitionFanout).boxed().toList(), divide(partitionFanout, maxSplits, CEILING))
                    .stream()
                    .filter(Predicate.not(List::isEmpty))
                    .map(partitionNumbers -> new JdbcSplit(
                            Optional.of(PARTITION_PREDICATE_PATTERN.formatted(
                                    quoteForSqlServer(functionName),
                                    quoteForSqlServer(columnName),
                                    partitionNumbers.get(0),
                                    partitionNumbers.get(partitionNumbers.size() - 1))),
                            dynamicFilter))
                    .collect(toImmutableList());
        }

        public static PartitionSplitBuilder builder()
        {
            return new PartitionSplitBuilder();
        }

        private String quoteForSqlServer(String name)
        {
            return "\"" + name.replace("\"", "\"\"") + "\"";
        }
    }
}
