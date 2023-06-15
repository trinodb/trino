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
package io.trino.plugin.hive.metastore.glue;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.RetryPolicy;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.hdfs.DynamicHdfsConfiguration;
import io.trino.hdfs.HdfsConfig;
import io.trino.hdfs.HdfsConfiguration;
import io.trino.hdfs.HdfsConfigurationInitializer;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.hdfs.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.aws.AwsApiCallStats;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.glue.converter.GlueInputConverter;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.type.Type;
import jakarta.annotation.Nullable;
import org.apache.hadoop.fs.Path;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GlueResponse;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.paginators.GetPartitionsPublisher;

import java.time.Duration;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Comparators.lexicographical;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.VIRTUAL_VIEW;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncPaginatedRequest;
import static io.trino.plugin.hive.metastore.glue.AwsSdkUtil.awsSyncRequest;
import static io.trino.plugin.hive.metastore.glue.GlueClientUtil.createAsyncGlueClient;
import static io.trino.plugin.hive.metastore.glue.converter.GlueInputConverter.convertPartition;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.getTableTypeNullable;
import static io.trino.plugin.hive.metastore.glue.converter.GlueToTrinoConverter.mappedCopy;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.function.Predicate.not;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

public class GlueHiveMetastore
        implements HiveMetastore
{
    private static final Logger log = Logger.get(GlueHiveMetastore.class);

    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "presto";
    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    private static final int BATCH_CREATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 1000;
    private static final Comparator<Iterable<String>> PARTITION_VALUE_COMPARATOR = lexicographical(String.CASE_INSENSITIVE_ORDER);
    private static final Predicate<software.amazon.awssdk.services.glue.model.Table> VIEWS_FILTER = table -> VIRTUAL_VIEW.name().equals(getTableTypeNullable(table));
    private static final RetryPolicy<?> CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(throwable -> Throwables.getRootCause(throwable) instanceof ConcurrentModificationException)
            .withDelay(Duration.ofMillis(100))
            .withMaxRetries(3)
            .build();

    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final GlueAsyncClient glueClient;
    private final Optional<String> defaultDir;
    private final int partitionSegments;
    private final Executor partitionsReadExecutor;
    private final GlueMetastoreStats stats;
    private final GlueColumnStatisticsProvider columnStatisticsProvider;
    private final boolean assumeCanonicalPartitionKeys;
    private final Predicate<software.amazon.awssdk.services.glue.model.Table> tableFilter;

    @Inject
    public GlueHiveMetastore(
            HdfsEnvironment hdfsEnvironment,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor partitionsReadExecutor,
            GlueColumnStatisticsProviderFactory columnStatisticsProviderFactory,
            GlueAsyncClient glueClient,
            @ForGlueHiveMetastore GlueMetastoreStats stats,
            @ForGlueHiveMetastore Predicate<software.amazon.awssdk.services.glue.model.Table> tableFilter)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.partitionsReadExecutor = requireNonNull(partitionsReadExecutor, "partitionsReadExecutor is null");
        this.assumeCanonicalPartitionKeys = glueConfig.isAssumeCanonicalPartitionKeys();
        this.tableFilter = requireNonNull(tableFilter, "tableFilter is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.columnStatisticsProvider = columnStatisticsProviderFactory.createGlueColumnStatisticsProvider(glueClient, stats);
    }

    @VisibleForTesting
    public static GlueHiveMetastore createTestingGlueHiveMetastore(java.nio.file.Path defaultWarehouseDir)
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new DynamicHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        GlueMetastoreStats stats = new GlueMetastoreStats();
        GlueHiveMetastoreConfig glueConfig = new GlueHiveMetastoreConfig()
                .setDefaultWarehouseDir(defaultWarehouseDir.toUri().toString());
        return new GlueHiveMetastore(
                hdfsEnvironment,
                glueConfig,
                directExecutor(),
                new DefaultGlueColumnStatisticsProviderFactory(directExecutor(), directExecutor()),
                createAsyncGlueClient(glueConfig, DefaultCredentialsProvider.create(), Optional.empty(), stats.newRequestMetricsPublisher()),
                stats,
                table -> true);
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            GetDatabaseResponse result = awsSyncRequest(glueClient::getDatabase,
                    GetDatabaseRequest.builder().name(databaseName).build(),
                    stats.getGetDatabase());
            return Optional.of(GlueToTrinoConverter.convertDatabase(result.database()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            ImmutableList.Builder<String> databaseNames = ImmutableList.builder();
            awsSyncPaginatedRequest(glueClient.getDatabasesPaginator(GetDatabasesRequest.builder().build()),
                    getDatabasesResponse -> getDatabasesResponse.databaseList().forEach(database -> databaseNames.add(database.name())),
                    stats.getGetDatabases());

            return databaseNames.build();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            GetTableResponse result = awsSyncRequest(glueClient::getTable,
                    GetTableRequest.builder().databaseName(databaseName).name(tableName).build(),
                    stats.getGetTable());
            return Optional.of(GlueToTrinoConverter.convertTable(result.table(), databaseName));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return columnStatisticsProvider.getSupportedColumnStatistics(type);
    }

    private Table getExistingTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public PartitionStatistics getTableStatistics(Table table)
    {
        return new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), columnStatisticsProvider.getTableColumnStatistics(table));
    }

    @Override
    public Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        Map<String, PartitionStatistics> partitionBasicStatistics = columnStatisticsProvider.getPartitionColumnStatistics(partitions).entrySet().stream()
                .collect(toImmutableMap(
                        entry -> makePartitionName(table, entry.getKey()),
                        entry -> new PartitionStatistics(getHiveBasicStatistics(entry.getKey().getParameters()), entry.getValue())));

        long tableRowCount = partitionBasicStatistics.values().stream()
                .mapToLong(partitionStatistics -> partitionStatistics.getBasicStatistics().getRowCount().orElse(0))
                .sum();
        if (!partitionBasicStatistics.isEmpty() && tableRowCount == 0) {
            // When the table has partitions, but row count statistics are set to zero, we treat this case as empty
            // statistics to avoid underestimation in the CBO. This scenario may be caused when other engines are
            // used to ingest data into partitioned hive tables.
            partitionBasicStatistics = partitionBasicStatistics.keySet().stream()
                    .map(key -> new SimpleEntry<>(key, PartitionStatistics.empty()))
                    .collect(toImmutableMap(SimpleEntry::getKey, SimpleEntry::getValue));
        }

        return partitionBasicStatistics;
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        Table table = getExistingTable(databaseName, tableName);
        if (transaction.isAcidTransactionRunning()) {
            table = Table.builder(table).setWriteId(OptionalLong.of(transaction.getWriteId())).build();
        }
        PartitionStatistics currentStatistics = getTableStatistics(table);
        PartitionStatistics updatedStatistics = update.apply(currentStatistics);

        try {
            TableInput tableInput = GlueInputConverter.convertTable(table);
            final Map<String, String> statisticsParameters = updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics());
            table = Table.builder(table).setParameters(statisticsParameters).build();
            awsSyncRequest(glueClient::updateTable, UpdateTableRequest.builder()
                            .databaseName(databaseName)
                            .tableInput(tableInput.toBuilder().parameters(statisticsParameters).build())
                            .build(), stats.getUpdateTable());
            columnStatisticsProvider.updateTableColumnStatistics(table, updatedStatistics.getColumnStatistics());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        Iterables.partition(updates.entrySet(), BATCH_CREATE_PARTITION_MAX_PAGE_SIZE).forEach(partitionUpdates ->
                updatePartitionStatisticsBatch(table, partitionUpdates.stream().collect(toImmutableMap(Entry::getKey, Entry::getValue))));
    }

    private void updatePartitionStatisticsBatch(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        ImmutableList.Builder<BatchUpdatePartitionRequestEntry> partitionUpdateRequests = ImmutableList.builder();
        ImmutableSet.Builder<GlueColumnStatisticsProvider.PartitionStatisticsUpdate> columnStatisticsUpdates = ImmutableSet.builder();

        Map<List<String>, String> partitionValuesToName = updates.keySet().stream()
                .collect(toImmutableMap(HiveUtil::toPartitionValues, identity()));

        List<Partition> partitions = batchGetPartition(table, ImmutableList.copyOf(updates.keySet()));
        Map<String, PartitionStatistics> partitionsStatistics = getPartitionStatistics(table, partitions);

        partitions.forEach(partition -> {
            Function<PartitionStatistics, PartitionStatistics> update = updates.get(partitionValuesToName.get(partition.getValues()));

            PartitionStatistics currentStatistics = partitionsStatistics.get(makePartitionName(table, partition));
            PartitionStatistics updatedStatistics = update.apply(currentStatistics);

            Map<String, String> updatedStatisticsParameters = updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics());

            partition = Partition.builder(partition).setParameters(updatedStatisticsParameters).build();
            Map<String, HiveColumnStatistics> updatedColumnStatistics = updatedStatistics.getColumnStatistics();

            PartitionInput partitionInput = GlueInputConverter.convertPartition(partition);

            partitionUpdateRequests.add(BatchUpdatePartitionRequestEntry.builder()
                    .partitionValueList(partition.getValues())
                    .partitionInput(partitionInput.toBuilder().parameters(partition.getParameters()).build())
                    .build());
            columnStatisticsUpdates.add(new GlueColumnStatisticsProvider.PartitionStatisticsUpdate(partition, updatedColumnStatistics));
        });

        List<List<BatchUpdatePartitionRequestEntry>> partitionUpdateRequestsPartitioned = Lists.partition(partitionUpdateRequests.build(), BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE);
        List<CompletableFuture<BatchUpdatePartitionResponse>> partitionUpdateRequestsFutures = new ArrayList<>();
        partitionUpdateRequestsPartitioned.forEach(partitionUpdateRequestsPartition -> {
            // Update basic statistics
            StatsRecordingAsyncHandler statsRecorder = new StatsRecordingAsyncHandler(stats.getBatchUpdatePartition());
            partitionUpdateRequestsFutures.add(glueClient.batchUpdatePartition(BatchUpdatePartitionRequest.builder()
                            .databaseName(table.getDatabaseName())
                            .tableName(table.getTableName())
                            .entries(partitionUpdateRequestsPartition).build())
                    .whenCompleteAsync((result, error) -> {
                        if (result != null) {
                            statsRecorder.onSuccess(result);
                        }
                        else if (error != null) {
                            statsRecorder.onError(error);
                        }
                    }));
        });

        try {
            // Update column statistics
            columnStatisticsProvider.updatePartitionStatistics(columnStatisticsUpdates.build());
            // Don't block on the batch update call until the column statistics have finished updating
            partitionUpdateRequestsFutures.forEach(MoreFutures::getFutureValue);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public List<String> getAllTables(String databaseName)
    {
        try {
            ImmutableList.Builder<String> tableNamesBuilder = ImmutableList.builder();
            awsSyncPaginatedRequest(glueClient.getTablesPaginator(GetTablesRequest.builder().databaseName(databaseName).build()),
                    tables -> {
                        tables.tableList().stream()
                                .filter(tableFilter)
                                .map(software.amazon.awssdk.services.glue.model.Table::name)
                                .forEach(tableNamesBuilder::add);
                    },
                    stats.getGetTables());
            return tableNamesBuilder.build();
        }
        catch (EntityNotFoundException | AccessDeniedException e) {
            // database does not exist or permission denied
            return ImmutableList.of();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return Optional.empty();
    }

    @Override
    public synchronized List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        return getAllViews(databaseName, table -> parameterValue.equals(getTableParameters(table).get(parameterKey)));
    }

    @Override
    public List<String> getAllViews(String databaseName)
    {
        return getAllViews(databaseName, table -> true);
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return Optional.empty();
    }

    private List<String> getAllViews(String databaseName, Predicate<software.amazon.awssdk.services.glue.model.Table> additionalFilter)
    {
        try {
            ImmutableList.Builder<String> viewsBuilder = ImmutableList.builder();
            awsSyncPaginatedRequest(glueClient.getTablesPaginator(GetTablesRequest.builder().databaseName(databaseName).build()),
                    tables -> {
                        tables.tableList().stream()
                                .filter(VIEWS_FILTER.and(additionalFilter))
                                .map(software.amazon.awssdk.services.glue.model.Table::name)
                                .forEach(viewsBuilder::add);
                    },
                    stats.getGetTables());
            return viewsBuilder.build();
        }
        catch (EntityNotFoundException | AccessDeniedException e) {
            // database does not exist or permission denied
            return ImmutableList.of();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        if (database.getLocation().isEmpty() && defaultDir.isPresent()) {
            String databaseLocation = new Path(defaultDir.get(), escapeSchemaName(database.getDatabaseName())).toString();
            database = Database.builder(database)
                    .setLocation(Optional.of(databaseLocation))
                    .build();
        }

        try {
            DatabaseInput databaseInput = GlueInputConverter.convertDatabase(database);
            awsSyncRequest(glueClient::createDatabase, CreateDatabaseRequest.builder()
                    .databaseInput(databaseInput)
                    .build(), stats.getCreateDatabase());
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getDatabaseName());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        if (database.getLocation().isPresent()) {
            HiveWriteUtils.createDirectory(hdfsContext, hdfsEnvironment, new Path(database.getLocation().get()));
        }
    }

    // TODO: respect deleteData
    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        Optional<String> location = Optional.empty();
        if (deleteData) {
            location = getDatabase(databaseName)
                    .orElseThrow(() -> new SchemaNotFoundException(databaseName))
                    .getLocation();
        }

        try {
            awsSyncRequest(glueClient::deleteDatabase, DeleteDatabaseRequest.builder()
                    .name(databaseName)
                    .build(), stats.getDeleteDatabase());
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        if (deleteData) {
            location.ifPresent(path -> deleteDir(hdfsContext, hdfsEnvironment, new Path(path), true));
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database);
            awsSyncRequest(glueClient::updateDatabase, UpdateDatabaseRequest.builder()
                            .name(databaseName)
                            .databaseInput(renamedDatabase.toBuilder().name(newDatabaseName).build())
                            .build(), stats.getUpdateDatabase());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setting the database owner is not supported by Glue");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = GlueInputConverter.convertTable(table);
            awsSyncRequest(glueClient::createTable, CreateTableRequest.builder()
                    .databaseName(table.getDatabaseName())
                    .tableInput(input)
                    .build(), stats.getCreateTable());
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Table table = getExistingTable(databaseName, tableName);

        try {
            Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY)
                    .run(() -> awsSyncRequest(glueClient::deleteTable, DeleteTableRequest.builder()
                            .databaseName(databaseName)
                            .name(tableName)
                            .build(), stats.getDeleteTable()));
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        Optional<String> location = table.getStorage().getOptionalLocation()
                .filter(not(String::isEmpty));
        if (deleteData && isManagedTable(table) && location.isPresent()) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(location.get()), true);
        }
    }

    private static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals(MANAGED_TABLE.name());
    }

    private static void deleteDir(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path, boolean recursive)
    {
        try {
            hdfsEnvironment.getFileSystem(context, path).delete(path, recursive);
        }
        catch (Exception e) {
            // don't fail if unable to delete path
            log.warn(e, "Failed to delete path: %s", path);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        if (!tableName.equals(newTable.getTableName()) || !databaseName.equals(newTable.getDatabaseName())) {
            throw new TrinoException(NOT_SUPPORTED, "Table rename is not yet supported by Glue service");
        }
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            awsSyncRequest(glueClient::updateTable, UpdateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(newTableInput)
                    .build(), stats.getUpdateTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        boolean newTableCreated = false;
        try {
            GetTableRequest getTableRequest = GetTableRequest.builder()
                    .databaseName(databaseName).name(tableName).build();
            GetTableResponse glueTable = awsSyncRequest(glueClient::getTable, getTableRequest, stats.getGetTable());
            TableInput tableInput = convertGlueTableToTableInput(glueTable.table(), newTableName);
            CreateTableRequest createTableRequest = CreateTableRequest.builder()
                    .databaseName(newDatabaseName)
                    .tableInput(tableInput)
                    .build();
            awsSyncRequest(glueClient::createTable, createTableRequest, stats.getCreateTable());
            newTableCreated = true;
            dropTable(databaseName, tableName, false);
        }
        catch (RuntimeException e) {
            if (newTableCreated) {
                try {
                    dropTable(databaseName, tableName, false);
                }
                catch (RuntimeException cleanupException) {
                    if (!cleanupException.equals(e)) {
                        e.addSuppressed(cleanupException);
                    }
                }
            }
            throw e;
        }
    }

    private TableInput convertGlueTableToTableInput(software.amazon.awssdk.services.glue.model.Table glueTable, String newTableName)
    {
        return TableInput.builder()
                .name(newTableName)
                .description(glueTable.description())
                .owner(glueTable.owner())
                .lastAccessTime(glueTable.lastAccessTime())
                .lastAnalyzedTime(glueTable.lastAnalyzedTime())
                .retention(glueTable.retention())
                .storageDescriptor(glueTable.storageDescriptor())
                .partitionKeys(glueTable.partitionKeys())
                .viewOriginalText(glueTable.viewOriginalText())
                .viewExpandedText(glueTable.viewExpandedText())
                .tableType(getTableTypeNullable(glueTable))
                .targetTable(glueTable.targetTable())
                .parameters(getTableParameters(glueTable)).build();
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table comment is not yet supported by Glue service");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        // TODO Add role support https://github.com/trinodb/trino/issues/5706
        if (principal.getType() != USER) {
            throw new TrinoException(NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }

        try {
            Table table = getExistingTable(databaseName, tableName);
            TableInput newTableInput = GlueInputConverter.convertTable(table);
            UpdateTableRequest updateTableRequest = UpdateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(newTableInput.toBuilder().owner(principal.getName()).build())
                    .build();
            awsSyncRequest(glueClient::updateTable, updateTableRequest, stats.getUpdateTable());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column comment is not yet supported by Glue service");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        Table newTable = Table.builder(oldTable)
                .addDataColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        if (oldTable.getPartitionColumns().stream().anyMatch(c -> c.getName().equals(oldColumnName))) {
            throw new TrinoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        for (Column column : oldTable.getDataColumns()) {
            if (column.getName().equals(oldColumnName)) {
                newDataColumns.add(new Column(newColumnName, column.getType(), column.getComment()));
            }
            else {
                newDataColumns.add(column);
            }
        }

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        verifyCanDropColumn(this, databaseName, tableName, columnName);
        Table oldTable = getExistingTable(databaseName, tableName);

        if (oldTable.getColumn(columnName).isEmpty()) {
            SchemaTableName name = new SchemaTableName(databaseName, tableName);
            throw new ColumnNotFoundException(name, columnName);
        }

        ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
        oldTable.getDataColumns().stream()
                .filter(fieldSchema -> !fieldSchema.getName().equals(columnName))
                .forEach(newDataColumns::add);

        Table newTable = Table.builder(oldTable)
                .setDataColumns(newDataColumns.build())
                .build();
        replaceTable(databaseName, tableName, newTable, null);
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        try {
            GetPartitionRequest getPartitionRequest = GetPartitionRequest.builder()
                    .databaseName(table.getDatabaseName())
                    .tableName(table.getTableName())
                    .partitionValues(partitionValues).build();
            GetPartitionResponse result = awsSyncRequest(glueClient::getPartition, getPartitionRequest, stats.getGetPartition());
            return Optional.of(new GluePartitionConverter(table).apply(result.partition()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        if (partitionKeysFilter.isNone()) {
            return Optional.of(ImmutableList.of());
        }
        String expression = GlueExpressionUtil.buildGlueExpression(columnNames, partitionKeysFilter, assumeCanonicalPartitionKeys);
        List<List<String>> partitionValues = getPartitionValues(databaseName, tableName, expression);
        return Optional.of(buildPartitionNames(columnNames, partitionValues));
    }

    private List<List<String>> getPartitionValues(String databaseName, String tableName, String expression)
    {
        if (partitionSegments == 1) {
            return getPartitionValues(databaseName, tableName, expression, null);
        }

        // Do parallel partition fetch.
        CompletionService<List<List<String>>> completionService = new ExecutorCompletionService<>(partitionsReadExecutor);
        List<Future<?>> futures = new ArrayList<>(partitionSegments);
        List<List<String>> partitions = new ArrayList<>();
        try {
            for (int i = 0; i < partitionSegments; i++) {
                Segment segment = Segment.builder().segmentNumber(i).totalSegments(partitionSegments).build();
                futures.add(completionService.submit(() -> getPartitionValues(databaseName, tableName, expression, segment)));
            }
            for (int i = 0; i < partitionSegments; i++) {
                Future<List<List<String>>> futurePartitions = completionService.take();
                partitions.addAll(futurePartitions.get());
            }
            // All futures completed normally
            futures.clear();
        }
        catch (ExecutionException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to fetch partitions from Glue Data Catalog", e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            futures.forEach(future -> future.cancel(true));
        }

        partitions.sort(PARTITION_VALUE_COMPARATOR);
        return partitions;
    }

    private List<List<String>> getPartitionValues(String databaseName, String tableName, String expression, @Nullable Segment segment)
    {
        try {
            GetPartitionsRequest partitionsRequest = GetPartitionsRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .expression(expression)
                    .segment(segment)
                    // We are interested in the partition values and excluding column schema
                    // avoids the problem of a large response.
                    .excludeColumnSchema(true)
                    .maxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS).build();

            ImmutableList.Builder<List<String>> partitionValuesBuilder = ImmutableList.builder();
            GetPartitionsPublisher partitionsPaginator = glueClient.getPartitionsPaginator(partitionsRequest);
            awsSyncPaginatedRequest(partitionsPaginator,
                    getPartitionsResponse -> {
                        getPartitionsResponse.partitions().stream()
                                .map(software.amazon.awssdk.services.glue.model.Partition::values)
                                .forEach(partitionValuesBuilder::add);
                    },
                    stats.getGetPartitions());

            return partitionValuesBuilder.build();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static List<String> buildPartitionNames(List<String> partitionColumns, List<List<String>> partitions)
    {
        return mappedCopy(partitions, partition -> toPartitionName(partitionColumns, partition));
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b']
     *     Partition names = ['a=1/b=2', 'a=2/b=2']
     * </pre>
     *
     * @param partitionNames List of full partition names
     * @return Mapping of partition name to partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return stats.getGetPartitionByName().call(() -> getPartitionsByNamesInternal(table, partitionNames));
    }

    private Map<String, Optional<Partition>> getPartitionsByNamesInternal(Table table, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        List<Partition> partitions = batchGetPartition(table, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(toMap(identity(), HiveUtil::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(Partition::getValues, identity()));

        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.buildOrThrow();
    }

    private List<Partition> batchGetPartition(Table table, List<String> partitionNames)
    {
        List<CompletableFuture<BatchGetPartitionResponse>> batchGetPartitionFutures = new ArrayList<>();
        try {
            List<PartitionValueList> pendingPartitions = partitionNames.stream()
                    .map(partitionName -> PartitionValueList.builder().values(toPartitionValues(partitionName)).build())
                    .collect(toCollection(ArrayList::new));

            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());

            // Reuse immutable field instances opportunistically between partitions
            GluePartitionConverter converter = new GluePartitionConverter(table);

            while (!pendingPartitions.isEmpty()) {
                for (List<PartitionValueList> partitions : Lists.partition(pendingPartitions, BATCH_GET_PARTITION_MAX_PAGE_SIZE)) {
                    StatsRecordingAsyncHandler statsRecorder = new StatsRecordingAsyncHandler(stats.getGetPartitions());
                    batchGetPartitionFutures.add(glueClient.batchGetPartition(BatchGetPartitionRequest.builder()
                                    .databaseName(table.getDatabaseName())
                                    .tableName(table.getTableName())
                                    .partitionsToGet(partitions).build())
                            .whenCompleteAsync((result, error) -> {
                                if (result != null) {
                                    statsRecorder.onSuccess(result);
                                }
                                else if (error != null) {
                                    statsRecorder.onError(error);
                                }
                            }));
                }
                pendingPartitions.clear();

                for (Future<BatchGetPartitionResponse> futureResponse : batchGetPartitionFutures) {
                    BatchGetPartitionResponse batchGetPartitionResponse = futureResponse.get();
                    List<software.amazon.awssdk.services.glue.model.Partition> partitions = batchGetPartitionResponse.partitions();
                    List<PartitionValueList> unprocessedKeys = batchGetPartitionResponse.unprocessedKeys();

                    // In the unlikely scenario where batchGetPartition call cannot make progress on retrieving partitions, avoid infinite loop
                    if (partitions.isEmpty()) {
                        verify(!unprocessedKeys.isEmpty(), "Empty unprocessedKeys for non-empty BatchGetPartitionRequest and empty partitions result");
                        throw new TrinoException(HIVE_METASTORE_ERROR, "Cannot make progress retrieving partitions. Unable to retrieve partitions: " + unprocessedKeys);
                    }

                    partitions.stream()
                            .map(converter)
                            .forEach(resultsBuilder::add);
                    pendingPartitions.addAll(unprocessedKeys);
                }
                batchGetPartitionFutures.clear();
            }

            return resultsBuilder.build();
        }
        catch (AwsServiceException | InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            // Ensure any futures still running are canceled in case of failure
            batchGetPartitionFutures.forEach(future -> future.cancel(true));
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        try {
            stats.getCreatePartitions().call(() -> {
                List<CompletableFuture<BatchCreatePartitionResponse>> futures = new ArrayList<>();

                for (List<PartitionWithStatistics> partitionBatch : Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)) {
                    List<PartitionInput> partitionInputs = mappedCopy(partitionBatch, GlueInputConverter::convertPartition);
                    StatsRecordingAsyncHandler statsRecorder = new StatsRecordingAsyncHandler(stats.getBatchCreatePartition());
                    futures.add(glueClient.batchCreatePartition(BatchCreatePartitionRequest.builder()
                                    .databaseName(databaseName)
                                    .tableName(tableName)
                                    .partitionInputList(partitionInputs).build())
                            .whenCompleteAsync((result, error) -> {
                                if (result != null) {
                                    statsRecorder.onSuccess(result);
                                }
                                else if (error != null) {
                                    statsRecorder.onError(error);
                                }
                            }));
                }
                for (Future<BatchCreatePartitionResponse> futureResponse : futures) {
                    try {
                        BatchCreatePartitionResponse result = futureResponse.get();
                        propagatePartitionErrorToTrinoException(databaseName, tableName, result.errors());
                    }
                    catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TrinoException(HIVE_METASTORE_ERROR, e);
                    }
                }

                Set<GlueColumnStatisticsProvider.PartitionStatisticsUpdate> updates = partitions.stream()
                        .map(partitionWithStatistics -> new GlueColumnStatisticsProvider.PartitionStatisticsUpdate(
                                partitionWithStatistics.getPartition(),
                                partitionWithStatistics.getStatistics().getColumnStatistics()))
                        .collect(toImmutableSet());
                columnStatisticsProvider.updatePartitionStatistics(updates);

                return null;
            });
        }
        catch (AwsServiceException | ExecutionException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToTrinoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.get(0).errorDetail();
            String glueExceptionCode = errorDetail.errorCode();

            switch (glueExceptionCode) {
                case "AlreadyExistsException":
                    throw new TrinoException(ALREADY_EXISTS, errorDetail.errorMessage());
                case "EntityNotFoundException":
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), errorDetail.errorMessage());
                default:
                    throw new TrinoException(HIVE_METASTORE_ERROR, errorDetail.errorCode() + ": " + errorDetail.errorMessage());
            }
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        Table table = getExistingTable(databaseName, tableName);
        Partition partition = getPartition(table, parts)
                .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts));

        try {
            awsSyncRequest(glueClient::deletePartition, DeletePartitionRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(parts)
                    .build(), stats.getDeletePartition());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(partLocation)) {
            deleteDir(hdfsContext, hdfsEnvironment, new Path(partLocation), true);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = convertPartition(partition);
            awsSyncRequest(glueClient::updatePartition, UpdatePartitionRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionInput(newPartition)
                    .partitionValueList(partition.getPartition().getValues()).build(), stats.getUpdatePartition());
            columnStatisticsProvider.updatePartitionStatistics(
                    partition.getPartition(),
                    partition.getStatistics().getColumnStatistics());
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "createRole is not supported by Glue");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "dropRole is not supported by Glue");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of(PUBLIC_ROLE_NAME);
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantRoles is not supported by Glue");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeRoles is not supported by Glue");
    }

    @Override
    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "listPrincipals is not supported by Glue");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal.toTrinoPrincipal(), PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by Glue");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "revokeTablePrivileges is not supported by Glue");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public void checkSupportsTransactions()
    {
        throw new TrinoException(NOT_SUPPORTED, "Glue does not support ACID tables");
    }

    static class StatsRecordingAsyncHandler
    {
        private final AwsApiCallStats stats;
        private final Stopwatch stopwatch;

        public StatsRecordingAsyncHandler(AwsApiCallStats stats)
        {
            this.stats = requireNonNull(stats, "stats is null");
            this.stopwatch = Stopwatch.createStarted();
        }

        public void onError(Throwable e)
        {
            stats.recordCall(stopwatch.elapsed(NANOSECONDS), true);
        }

        public void onSuccess(GlueResponse response)
        {
            stats.recordCall(stopwatch.elapsed(NANOSECONDS), false);
        }
    }
}
