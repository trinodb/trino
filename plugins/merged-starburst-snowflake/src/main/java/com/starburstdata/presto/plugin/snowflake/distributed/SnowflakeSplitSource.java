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
import com.google.common.util.concurrent.ListeningExecutorService;
import com.starburstdata.presto.plugin.snowflake.jdbc.SnowflakeClient;
import io.airlift.log.Logger;
import io.airlift.stats.CounterStat;
import io.airlift.units.Duration;
import io.prestosql.plugin.hive.CachingDirectoryLister;
import io.prestosql.plugin.hive.HdfsConfig;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveConfig;
import io.prestosql.plugin.hive.HiveMetastoreClosure;
import io.prestosql.plugin.hive.HivePartition;
import io.prestosql.plugin.hive.HivePartitionManager;
import io.prestosql.plugin.hive.HiveSplit;
import io.prestosql.plugin.hive.HiveSplitManager;
import io.prestosql.plugin.hive.HiveTableHandle;
import io.prestosql.plugin.hive.NamenodeStats;
import io.prestosql.plugin.hive.metastore.Column;
import io.prestosql.plugin.hive.metastore.HiveMetastore;
import io.prestosql.plugin.hive.metastore.SemiTransactionalHiveMetastore;
import io.prestosql.plugin.hive.metastore.Storage;
import io.prestosql.plugin.hive.metastore.StorageFormat;
import io.prestosql.plugin.hive.metastore.Table;
import io.prestosql.plugin.jdbc.JdbcClient;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.connector.ConnectorPartitionHandle;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.SchemaTableName;
import io.prestosql.spi.type.TypeManager;
import net.jodah.failsafe.Failsafe;
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
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getLast;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHdfsEnvironment;
import static com.starburstdata.presto.plugin.snowflake.distributed.HiveUtils.getHiveColumnHandles;
import static com.starburstdata.presto.plugin.snowflake.distributed.SnowflakeHiveTypeTranslator.toHiveType;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.prestosql.plugin.hive.HiveStorageFormat.PARQUET;
import static io.prestosql.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.prestosql.spi.connector.ConnectorSplitManager.SplitSchedulingStrategy.UNGROUPED_SCHEDULING;
import static io.prestosql.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static io.prestosql.spi.predicate.TupleDomain.all;
import static io.prestosql.spi.predicate.TupleDomain.none;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.TimeUnit.SECONDS;
import static net.snowflake.client.jdbc.cloud.storage.StageInfo.StageType.S3;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.CONNECTION_DOES_NOT_EXIST;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.CONNECTION_EXCEPTION;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.DATA_EXCEPTION;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.INTERNAL_ERROR;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.IO_ERROR;
import static net.snowflake.client.jdbc.internal.snowflake.common.core.SqlState.PROGRAM_LIMIT_EXCEEDED;
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
    private static final String AWS_KEY_ID = "AWS_KEY_ID";
    private static final String AWS_SECRET_KEY = "AWS_SECRET_KEY";
    private static final String AWS_TOKEN = "AWS_TOKEN";

    private final ListeningExecutorService executorService;
    private final TypeManager typeManager;
    private final JdbcClient client;
    private final ConnectorSession session;
    private final JdbcTableHandle jdbcTableHandle;
    private final List<JdbcColumnHandle> columns;
    private final RetryPolicy<ConnectorSplitBatch> retryPolicy;

    private final SnowflakeConnectionManager connectionManager;
    private final SnowflakeDistributedConfig snowflakeConfig;
    private final SnowflakeExportStats exportStats;

    private SnowflakeFileTransferAgent transferAgent;
    private ConnectorSplitSource hiveSplitSource;

    SnowflakeSplitSource(
            ListeningExecutorService executorService,
            TypeManager typeManager,
            JdbcClient client,
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
                .handleIf(SnowflakeSplitSource::isIntermittentIssue);
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

        return hiveSplitSource.getNextBatch(NOT_PARTITIONED, maxSize)
                .thenApply(hiveSplitBatch -> new ConnectorSplitBatch(
                        hiveSplitBatch.getSplits().stream()
                                .map(hiveSplit -> new SnowflakeSplit(
                                        (HiveSplit) hiveSplit,
                                        getCredential(AWS_KEY_ID),
                                        getCredential(AWS_SECRET_KEY),
                                        getCredential(AWS_TOKEN),
                                        getQueryStageMasterKey()))
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
                    SnowflakeConnectionV1 connection = connectionManager.openConnection(QueryId.valueOf(session.getQueryId()), JdbcIdentity.from(session));
                    String stageName = "export_" + randomUUID().toString().replace("-", "_");
                    execute(connection, format("CREATE TEMPORARY STAGE %s.%s FILE_FORMAT = (TYPE = PARQUET)", snowflakeConfig.getStageSchema(), stageName));

                    try (PreparedStatement statement = new SnowflakeQueryBuilder(client).buildSql(
                            session,
                            connection,
                            jdbcTableHandle.getRemoteTableName(),
                            jdbcTableHandle.getGroupingSets(),
                            columns,
                            jdbcTableHandle.getConstraint(),
                            Optional.empty(),
                            tryApplyLimit(jdbcTableHandle.getLimit()).andThen(sql -> copyIntoStage(sql, stageName)))) {
                        statement.executeQuery();
                    }

                    transferAgent = new SnowflakeFileTransferAgent(
                            format("GET @%s.%s/ %s", snowflakeConfig.getStageSchema(), stageName, DUMMY_LOCATION),
                            connection.getSfSession(),
                            new SFStatement(connection.getSfSession()));
                    checkState(transferAgent.getStageInfo().getStageType() == S3, "Only S3 Snowflake Stages are supported");

                    return new ConnectorSplitBatch(ImmutableList.of(), false);
                }));
            }
            catch (Exception exception) {
                throw new PrestoException(JDBC_ERROR, exception);
            }
        }));
    }

    private <T> CheckedSupplier<T> measureExportAttemptTime(Callable<T> supplier)
    {
        return () -> exportStats.getTime().time(supplier);
    }

    private static boolean isIntermittentIssue(Throwable throwable)
    {
        if (!(throwable instanceof SnowflakeSQLException)) {
            return false;
        }

        SnowflakeSQLException snowflakeSQLException = (SnowflakeSQLException) throwable;
        return INTERMITTENT_SQL_STATES.contains(snowflakeSQLException.getSQLState());
    }

    private void closeSnowflakeConnection(ExecutionAttemptedEvent executionAttemptedEvent)
    {
        log.info("Retrying Snowflake S3 export (query id: %s, exception: %s", session.getQueryId(), executionAttemptedEvent.getLastFailure());
        exportStats.getTotalRetries().update(1);

        // close connector so that temporary stage is removed when retrying
        connectionManager.closeConnections(QueryId.valueOf(session.getQueryId()));
    }

    private ConnectorSplitSource getHiveSplitSource()
    {
        SchemaTableName schemaTableName = new SchemaTableName(jdbcTableHandle.getSchemaName(), jdbcTableHandle.getTableName());
        HiveTableHandle tableLayoutHandle = new HiveTableHandle(
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                Optional.of(ImmutableMap.of()),
                getHiveColumnHandles(columns),
                Optional.of(ImmutableList.of(new HivePartition(schemaTableName))),
                all(),
                none(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        return getHiveSplitManager().getSplits(
                // no transaction is needed
                null,
                session,
                tableLayoutHandle,
                // Snowflake connector does not support partitioning
                UNGROUPED_SCHEDULING,
                DynamicFilter.EMPTY);
    }

    private HiveSplitManager getHiveSplitManager()
    {
        HiveConfig hiveConfig = snowflakeConfig.getHiveConfig();
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsEnvironment hdfsEnvironment = getHdfsEnvironment(
                hdfsConfig,
                getCredential(AWS_KEY_ID),
                getCredential(AWS_SECRET_KEY),
                getCredential(AWS_TOKEN),
                Optional.of(getQueryStageMasterKey()));

        SemiTransactionalHiveMetastore metastore = getSemiTransactionalHiveMetastore(hiveConfig, hdfsEnvironment);
        metastore.beginQuery(session);

        return new HiveSplitManager(
                ignored -> metastore,
                new HivePartitionManager(hiveConfig),
                new NamenodeStats(),
                hdfsEnvironment,
                new CachingDirectoryLister(hiveConfig),
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
                jdbcTableHandle.getSchemaName(),
                jdbcTableHandle.getTableName(),
                "dummy",
                EXTERNAL_TABLE.name(),
                Storage.builder()
                        .setLocation("s3://" + transferAgent.getStageLocation())
                        .setStorageFormat(StorageFormat.fromHiveStorageFormat(PARQUET))
                        .build(),
                getHiveColumns(),
                ImmutableList.of(),
                ImmutableMap.of(),
                Optional.empty(),
                Optional.empty());
        HiveMetastore metastore = new SnowflakeHiveMetastore(table);
        return new SemiTransactionalHiveMetastore(
                hdfsEnvironment,
                new HiveMetastoreClosure(metastore),
                executorService,
                executorService,
                hiveConfig.isSkipDeletionForAlter(),
                hiveConfig.isSkipTargetCleanupOnRollback(),
                Optional.of(new Duration(5, SECONDS)),
                Executors.newScheduledThreadPool(1));
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

    private static Function<String, String> tryApplyLimit(OptionalLong limit)
    {
        if (limit.isEmpty()) {
            return Function.identity();
        }

        return sql -> SnowflakeClient.applyLimit(sql, limit.getAsLong());
    }

    private void execute(SnowflakeConnectionV1 connection, String sql)
            throws SQLException
    {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private String getCredential(String credentialName)
    {
        return requireNonNull(transferAgent.getStageInfo().getCredentials().get(credentialName), format("%s is null", credentialName)).toString();
    }

    private boolean isEmptyExport()
    {
        return transferAgent.getEncryptionMaterial().isEmpty();
    }

    private String getQueryStageMasterKey()
    {
        return getLast(transferAgent.getEncryptionMaterial()).getQueryStageMasterKey();
    }
}
