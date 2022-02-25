/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.presto.plugin.snowflake.distributed;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import io.trino.plugin.hive.CachingDirectoryLister;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveConfig;
import io.trino.plugin.hive.HiveMetastoreClosure;
import io.trino.plugin.hive.HivePartition;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveSplit;
import io.trino.plugin.hive.HiveSplitManager;
import io.trino.plugin.hive.HiveTableHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.HiveTransactionManager;
import io.trino.plugin.hive.NamenodeStats;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.trino.plugin.hive.metastore.Storage;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.plugin.jdbc.PreparedQuery;
import io.trino.spi.QueryId;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPartitionHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.TypeManager;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.FailsafeException;
import net.jodah.failsafe.RetryPolicy;
import net.jodah.failsafe.event.ExecutionAttemptedEvent;
import net.jodah.failsafe.function.CheckedSupplier;
import net.snowflake.client.core.SFStatement;
import net.snowflake.client.jdbc.SnowflakeConnectionV1;
import net.snowflake.client.jdbc.SnowflakeFileTransferAgent;
import net.snowflake.client.jdbc.SnowflakeSQLException;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHdfsEnvironment;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHiveColumnHandles;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getSnowflakeStageAccessInfo;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getStageLocation;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.validateStageType;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeDistributedSessionProperties.retryCanceledQueries;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeHiveTypeTranslator.toHiveType;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.acid.AcidTransaction.NO_ACID_TRANSACTION;
import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.trino.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.trino.spi.predicate.TupleDomain.all;
import static io.trino.spi.predicate.TupleDomain.none;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.CONNECTION_DOES_NOT_EXIST;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.CONNECTION_EXCEPTION;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.DATA_EXCEPTION;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.INTERNAL_ERROR;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.IO_ERROR;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.PROGRAM_LIMIT_EXCEEDED;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.QUERY_CANCELED;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.SYSTEM_ERROR;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;

public class SnowflakeSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(SnowflakeSplitSource.class);
    private static final Set<String> INTERMITTENT_SQL_STATES = ImmutableSet.of(
            INTERNAL_ERROR,
            SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
            DATA_EXCEPTION,
            SYSTEM_ERROR,
            PROGRAM_LIMIT_EXCEEDED,
            IO_ERROR,
            CONNECTION_EXCEPTION,
            CONNECTION_DOES_NOT_EXIST);

    private static final String DUMMY_LOCATION = "file:///tmp/dummy_location_presto_connector_tmp/";

    private final ListeningExecutorService executorService;
    private final TypeManager typeManager;
    private final SnowflakeClient client;
    private final ConnectorSession session;
    private final JdbcTableHandle jdbcTableHandle;
    private final List<JdbcColumnHandle> columns;
    private final RetryPolicy<ConnectorSplitBatch> retryPolicy;

    private final SnowflakeConnectionManager connectionManager;
    private final SnowflakeDistributedConfig snowflakeConfig;
    private final SnowflakeExportStats exportStats;

    private SnowflakeFileTransferAgent transferAgent;
    private ConnectorSplitSource hiveSplitSource;

    private final Set<String> retryableErrorCodes;
    private final CachingDirectoryLister directoryLister;

    SnowflakeSplitSource(
            ListeningExecutorService executorService,
            TypeManager typeManager,
            SnowflakeClient client,
            ConnectorSession session,
            JdbcTableHandle tableHandle,
            List<JdbcColumnHandle> columns,
            SnowflakeConnectionManager connectionManager,
            SnowflakeDistributedConfig snowflakeConfig,
            SnowflakeExportStats exportStats)
    {
        this.executorService = requireNonNull(executorService, "executorService is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.client = requireNonNull(client, "client is null");
        this.session = requireNonNull(session, "session is null");
        this.jdbcTableHandle = requireNonNull(tableHandle, "tableHandle is null");
        this.columns = requireNonNull(columns, "columns is null");
        this.connectionManager = requireNonNull(connectionManager, "connection is null");
        this.snowflakeConfig = requireNonNull(snowflakeConfig, "snowflakeConfig is null");
        this.exportStats = requireNonNull(exportStats, "exportStats is null");
        this.retryPolicy = new RetryPolicy<ConnectorSplitBatch>()
                .withMaxAttempts(snowflakeConfig.getMaxExportRetries())
                .onRetry(this::closeSnowflakeConnection)
                .onFailure(event -> exportStats.getTotalFailures().update(1))
                .handleIf(this::isIntermittentIssue);
        if (retryCanceledQueries(session)) {
            retryableErrorCodes = Sets.union(INTERMITTENT_SQL_STATES, ImmutableSet.of(QUERY_CANCELED));
        }
        else {
            retryableErrorCodes = INTERMITTENT_SQL_STATES;
        }
        // Exported tables are ephemeral, so directory listing cache would not be useful (and can only be harmful).
        // Cache is disabled because the list of tables to cache is empty.
        this.directoryLister = new CachingDirectoryLister(new Duration(1, SECONDS), 1, ImmutableList.of());
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
    {
        if (transferAgent == null) {
            return exportTableData();
        }

        if (isEmptyExport()) {
            return completedFuture(new ConnectorSplitBatch(ImmutableList.of(), true));
        }

        if (hiveSplitSource == null) {
            hiveSplitSource = getHiveSplitSource();
        }

        SnowflakeStageAccessInfo snowflakeStageAccessInfo = getSnowflakeStageAccessInfo(transferAgent);
        return hiveSplitSource.getNextBatch(NOT_PARTITIONED, maxSize)
                .thenApply(hiveSplitBatch -> new ConnectorSplitBatch(
                        hiveSplitBatch.getSplits().stream()
                                .map(hiveSplit -> new SnowflakeSplit((HiveSplit) hiveSplit, snowflakeStageAccessInfo))
                                .collect(toImmutableList()),
                        hiveSplitBatch.isNoMoreSplits()));
    }

    @Override
    public void close()
    {
        if (hiveSplitSource != null) {
            hiveSplitSource.close();
        }
    }

    @Override
    public boolean isFinished()
    {
        return (transferAgent != null && isEmptyExport()) ||
                (hiveSplitSource != null && hiveSplitSource.isFinished());
    }

    private CompletableFuture<ConnectorSplitBatch> exportTableData()
    {
        return toCompletableFuture(executorService.submit(() -> {
            try {
                return Failsafe.with(retryPolicy).get(measureExportAttemptTime(() -> {
                    SnowflakeConnectionV1 connection = connectionManager.openConnection(session);
                    String stageName = "export_" + randomUUID().toString().replace("-", "_");
                    execute(connection, format("CREATE TEMPORARY STAGE %s.%s FILE_FORMAT = (TYPE = PARQUET)", snowflakeConfig.getStageSchema(), stageName));

                    SnowflakeQueryBuilder queryBuilder = new SnowflakeQueryBuilder();
                    PreparedQuery preparedQuery = queryBuilder.prepareSelectQuery(
                            client,
                            session,
                            connection,
                            jdbcTableHandle.getRelationHandle(),
                            Optional.empty(),
                            columns,
                            ImmutableMap.of(),
                            jdbcTableHandle.getConstraint(),
                            Optional.empty());
                    preparedQuery = client.applyQueryTransformations(jdbcTableHandle, preparedQuery);
                    preparedQuery = preparedQuery.transformQuery(sql -> copyIntoStage(sql, stageName));
                    try (PreparedStatement statement = queryBuilder.prepareStatement(client, session, connection, preparedQuery)) {
                        // TODO close ResultSet
                        statement.executeQuery();
                    }

                    transferAgent = new SnowflakeFileTransferAgent(
                            format("GET @%s.%s/ %s", snowflakeConfig.getStageSchema(), stageName, DUMMY_LOCATION),
                            connection.getSfSession(),
                            new SFStatement(connection.getSfSession()));
                    validateStageType(transferAgent.getStageInfo().getStageType());
                    return new ConnectorSplitBatch(ImmutableList.of(), false);
                }));
            }
            catch (FailsafeException exception) {
                if (exception.getCause() instanceof SnowflakeSQLException) {
                    String snowflakeQueryId = ((SnowflakeSQLException) exception.getCause()).getQueryId();
                    throw new TrinoException(JDBC_ERROR, format("%s (Snowflake query id %s)", exception.getMessage(), snowflakeQueryId), exception);
                }
                throw new TrinoException(JDBC_ERROR, exception);
            }
            catch (Exception exception) {
                throw new TrinoException(JDBC_ERROR, exception);
            }
        }));
    }

    private <T> CheckedSupplier<T> measureExportAttemptTime(Callable<T> supplier)
    {
        return () -> exportStats.getTime().time(supplier);
    }

    private boolean isIntermittentIssue(Throwable throwable)
    {
        if (!(throwable instanceof SnowflakeSQLException)) {
            return false;
        }

        SnowflakeSQLException snowflakeSQLException = (SnowflakeSQLException) throwable;
        return retryableErrorCodes.contains(snowflakeSQLException.getSQLState());
    }

    private void closeSnowflakeConnection(ExecutionAttemptedEvent<?> executionAttemptedEvent)
    {
        Throwable lastFailure = executionAttemptedEvent.getLastFailure();
        String snowflakeQueryId = "unknown";
        if (lastFailure instanceof SnowflakeSQLException) {
            snowflakeQueryId = ((SnowflakeSQLException) lastFailure).getQueryId();
        }
        log.info("Retrying Snowflake S3 export (query id: %s, Snowflake query id: %s, exception: %s", session.getQueryId(), snowflakeQueryId, lastFailure);
        exportStats.getTotalRetries().update(1);

        // close connector so that temporary stage is removed when retrying
        connectionManager.closeConnections(QueryId.valueOf(session.getQueryId()));
    }

    private ConnectorSplitSource getHiveSplitSource()
    {
        SchemaTableName schemaTableName = new SchemaTableName("dummy_schema", "dummy_table");
        HiveTableHandle tableLayoutHandle = new HiveTableHandle(
                schemaTableName.getSchemaName(),
                schemaTableName.getTableName(),
                Optional.of(ImmutableMap.of()),
                ImmutableList.of(),
                getHiveColumnHandles(columns),
                Optional.of(ImmutableList.of(new HivePartition(schemaTableName))),
                all(),
                none(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                ImmutableSet.of(),
                ImmutableSet.of(),
                NO_ACID_TRANSACTION,
                false,
                Optional.empty());

        ConnectorTransactionHandle transactionHandle = new HiveTransactionHandle(false);
        return getHiveSplitManager(transactionHandle).getSplits(
                transactionHandle,
                session,
                tableLayoutHandle,
                // Snowflake connector does not support partitioning
                UNGROUPED_SCHEDULING,
                DynamicFilter.EMPTY);
    }

    private HiveSplitManager getHiveSplitManager(ConnectorTransactionHandle transactionHandle)
    {
        HiveConfig hiveConfig = snowflakeConfig.getHiveConfig();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(hdfsConfig, transferAgent);
        SemiTransactionalHiveMetastore metastore = getSemiTransactionalHiveMetastore(hiveConfig, hdfsEnvironment);
        metastore.beginQuery(session);
        HiveTransactionManager transactionManager = new HiveTransactionManager(new SnowflakeHiveTransactionalMetadataFactory(metastore));
        transactionManager.begin(transactionHandle);

        return new HiveSplitManager(
                transactionManager,
                new HivePartitionManager(hiveConfig),
                new NamenodeStats(),
                hdfsEnvironment,
                directoryLister,
                executorService,
                new CounterStat(),
                hiveConfig.getMaxOutstandingSplits(),
                hiveConfig.getMaxOutstandingSplitsSize(),
                hiveConfig.getMinPartitionBatchSize(),
                hiveConfig.getMaxPartitionBatchSize(),
                hiveConfig.getMaxInitialSplits(),
                hiveConfig.getSplitLoaderConcurrency(),
                hiveConfig.getMaxSplitsPerSecond(),
                hiveConfig.getRecursiveDirWalkerEnabled(),
                typeManager);
    }

    private SemiTransactionalHiveMetastore getSemiTransactionalHiveMetastore(HiveConfig hiveConfig, HdfsEnvironment hdfsEnvironment)
    {
        Table table = new Table(
                "dummy_schema",
                "dummy_table",
                Optional.of("dummy"),
                EXTERNAL_TABLE.name(),
                Storage.builder()
                        .setLocation(getStageLocation(transferAgent))
                        .setStorageFormat(StorageFormat.fromHiveStorageFormat(PARQUET))
                        .build(),
                getHiveColumns(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty(),
                OptionalLong.empty());
        HiveMetastore metastore = new SnowflakeHiveMetastore(table);
        return new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                new HiveMetastoreClosure(metastore),
                executorService,
                executorService,
                executorService,
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                hiveConfig.isDeleteSchemaLocationsFallback(),
                Optional.of(new Duration(5, SECONDS)),
                Executors.newScheduledThreadPool(1),
                directoryLister);
    }

    private List<Column> getHiveColumns()
    {
        return columns.stream()
                .map(column -> toHiveColumn(column))
                .collect(toImmutableList());
    }

    private Column toHiveColumn(JdbcColumnHandle jdbcColumnHandle)
    {
        return new Column(jdbcColumnHandle.getColumnName(), toHiveType(jdbcColumnHandle.getColumnType()), Optional.empty());
    }

    private String copyIntoStage(String sql, String stageName)
    {
        return format("COPY INTO @%s.%s FROM (%s) MAX_FILE_SIZE=%s HEADER=TRUE",
                snowflakeConfig.getStageSchema(),
                stageName,
                sql,
                snowflakeConfig.getExportFileMaxSize().toBytes());
    }

    private void execute(SnowflakeConnectionV1 connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private boolean isEmptyExport()
    {
        return transferAgent.getEncryptionMaterial().isEmpty();
    }
}
