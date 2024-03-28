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

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import dev.failsafe.Failsafe;
import dev.failsafe.FailsafeException;
import dev.failsafe.RetryPolicy;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HivePartitionManager;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.StatisticsUpdateMode;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;
import jakarta.annotation.Nullable;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.ColumnStatistics;
import software.amazon.awssdk.services.glue.model.ConcurrentModificationException;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.opentelemetry.context.Context.taskWrapping;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.fromGlueStatistics;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.toGlueColumnStatistics;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.toGlueDatabaseInput;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.toGlueFunctionInput;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.toGluePartitionInput;
import static io.trino.plugin.hive.metastore.glue.GlueConverter.toGlueTableInput;
import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Map.entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newThreadPerTaskExecutor;
import static java.util.function.Predicate.not;
import static java.util.function.UnaryOperator.identity;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toMap;

public class GlueHiveMetastore
        implements HiveMetastore
{
    private static final Logger log = Logger.get(GlueHiveMetastore.class);

    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String DEFAULT_METASTORE_USER = "trino";

    // Read limit for AWS Glue API GetColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-GetColumnStatisticsForPartition
    private static final int GLUE_COLUMN_READ_STAT_PAGE_SIZE = 100;

    // Write limit for AWS Glue API UpdateColumnStatisticsForPartition
    // https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-catalog-partitions.html#aws-glue-api-catalog-partitions-UpdateColumnStatisticsForPartition
    private static final int GLUE_COLUMN_WRITE_STAT_PAGE_SIZE = 25;

    private static final int BATCH_GET_PARTITION_MAX_PAGE_SIZE = 1000;
    private static final int AWS_GLUE_GET_PARTITIONS_MAX_RESULTS = 1000;
    private static final int BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE = 100;
    private static final int AWS_GLUE_GET_FUNCTIONS_MAX_RESULTS = 100;

    private static final RetryPolicy<Object> CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY = RetryPolicy.builder()
            .handleIf(throwable -> Throwables.getRootCause(throwable) instanceof ConcurrentModificationException)
            .withDelay(Duration.ofMillis(100))
            .withMaxRetries(3)
            .build();

    private final GlueClient glueClient;
    private final GlueContext glueContext;
    private final GlueCache glueCache;
    private final TrinoFileSystem fileSystem;
    private final Optional<String> defaultDir;
    private final int partitionSegments;
    private final boolean assumeCanonicalPartitionKeys;
    private final GlueMetastoreStats stats = new GlueMetastoreStats();
    private final Predicate<software.amazon.awssdk.services.glue.model.Table> tableVisibilityFilter;
    private final Executor executor;

    @Inject
    public GlueHiveMetastore(
            GlueClient glueClient,
            GlueContext glueContext,
            GlueCache glueCache,
            TrinoFileSystemFactory fileSystemFactory,
            GlueHiveMetastoreConfig config,
            Set<TableKind> visibleTableKinds)
    {
        this(
                glueClient,
                glueContext,
                glueCache,
                fileSystemFactory.create(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER)),
                config.getDefaultWarehouseDir(),
                config.getPartitionSegments(),
                config.isAssumeCanonicalPartitionKeys(),
                visibleTableKinds,
                taskWrapping(newThreadPerTaskExecutor(Thread.ofVirtual().name("glue-", 0L).factory())));
    }

    public GlueHiveMetastore(
            GlueClient glueClient,
            GlueContext glueContext,
            GlueCache glueCache, TrinoFileSystem fileSystem,
            Optional<String> defaultDir,
            int partitionSegments,
            boolean assumeCanonicalPartitionKeys,
            Set<TableKind> visibleTableKinds,
            Executor executor)
    {
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.glueContext = requireNonNull(glueContext, "glueContext is null");
        this.glueCache = glueCache;
        this.fileSystem = requireNonNull(fileSystem, "fileSystem is null");
        this.defaultDir = requireNonNull(defaultDir, "defaultDir is null");
        this.partitionSegments = partitionSegments;
        this.assumeCanonicalPartitionKeys = assumeCanonicalPartitionKeys;
        this.tableVisibilityFilter = createTablePredicate(visibleTableKinds);
        this.executor = requireNonNull(executor, "executor is null");
    }

    @Managed
    @Flatten
    public GlueMetastoreStats getStats()
    {
        return stats;
    }

    @Managed
    @Flatten
    public GlueCache getGlueCache()
    {
        return glueCache;
    }

    @Override
    public List<String> getAllDatabases()
    {
        return glueCache.getDatabaseNames(this::getDatabasesInternal);
    }

    private List<String> getDatabasesInternal(Consumer<Database> cacheDatabase)
    {
        try {
            return stats.getGetDatabases().call(() -> glueClient.getDatabasesPaginator(glueContext::configureClient).stream()
                    .map(GetDatabasesResponse::databaseList)
                    .flatMap(List::stream)
                    .map(GlueConverter::fromGlueDatabase)
                    .peek(cacheDatabase)
                    .map(Database::getDatabaseName)
                    .toList());
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        return glueCache.getDatabase(databaseName, () -> getDatabaseInternal(databaseName));
    }

    private Optional<Database> getDatabaseInternal(String databaseName)
    {
        try {
            GetDatabaseResponse response = stats.getGetDatabase().call(() -> glueClient.getDatabase(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .name(databaseName)));
            return Optional.of(GlueConverter.fromGlueDatabase(response.database()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        if (database.getLocation().isEmpty() && defaultDir.isPresent()) {
            Location location = Location.of(defaultDir.get())
                    .appendPath(escapeSchemaName(database.getDatabaseName()));
            database = Database.builder(database)
                    .setLocation(Optional.of(location.toString()))
                    .build();
        }

        try {
            DatabaseInput databaseInput = toGlueDatabaseInput(database);
            stats.getCreateDatabase().call(() -> glueClient.createDatabase(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseInput(databaseInput)));
        }
        catch (AlreadyExistsException e) {
            // Do not throw SchemaAlreadyExistsException if this query has already created the database.
            // This may happen when an actually successful metastore create call is retried
            // because of a timeout on our side.
            String expectedQueryId = database.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getDatabase(database.getDatabaseName())
                        .map(Database::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (expectedQueryId.equals(existingQueryId)) {
                    return;
                }
            }
            throw new SchemaAlreadyExistsException(database.getDatabaseName(), e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateDatabase(database.getDatabaseName());
            glueCache.invalidateDatabaseNames();
        }

        if (database.getLocation().isPresent()) {
            Location location = Location.of(database.getLocation().get());
            try {
                fileSystem.createDirectory(location);
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + location, e);
            }
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        Optional<Location> location = Optional.empty();
        if (deleteData) {
            location = getDatabase(databaseName)
                    .orElseThrow(() -> new SchemaNotFoundException(databaseName))
                    .getLocation()
                    .map(Location::of);
        }

        try {
            stats.getDeleteDatabase().call(() -> glueClient.deleteDatabase(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .name(databaseName)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateDatabase(databaseName);
            glueCache.invalidateDatabaseNames();
        }

        location.ifPresent(this::deleteDir);
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            var database = stats.getGetDatabase().call(() -> glueClient.getDatabase(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .name(databaseName)).database());
            DatabaseInput renamedDatabase = DatabaseInput.builder()
                    .name(newDatabaseName)
                    .parameters(database.parameters())
                    .description(database.description())
                    .locationUri(database.locationUri())
                    .build();
            stats.getUpdateDatabase().call(() -> glueClient.updateDatabase(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .name(databaseName).databaseInput(renamedDatabase)));
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(newDatabaseName, e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateDatabase(databaseName);
            glueCache.invalidateDatabase(newDatabaseName);
            glueCache.invalidateDatabaseNames();
        }
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "setting the database owner is not supported by Glue");
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return glueCache.getTables(databaseName, cacheTable -> getTablesInternal(cacheTable, databaseName));
    }

    private List<TableInfo> getTablesInternal(Consumer<Table> cacheTable, String databaseName)
    {
        return stats.getGetTables()
                .call(() -> glueClient.getTablesPaginator(builder -> builder
                                .applyMutation(glueContext::configureClient)
                                .databaseName(databaseName)).stream()
                        .map(GetTablesResponse::tableList)
                        .flatMap(List::stream))
                .filter(tableVisibilityFilter)
                .map(glueTable -> GlueConverter.fromGlueTable(glueTable, databaseName))
                .peek(cacheTable)
                .map(table -> new TableInfo(
                        new SchemaTableName(databaseName, table.getTableName()),
                        TableInfo.ExtendedRelationType.fromTableTypeAndComment(table.getTableType(), table.getParameters().get(TABLE_COMMENT))))
                .toList();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return glueCache.getTable(databaseName, tableName, () -> getTableInternal(databaseName, tableName));
    }

    private Optional<Table> getTableInternal(String databaseName, String tableName)
    {
        try {
            GetTableResponse result = stats.getGetTable().call(() -> glueClient.getTable(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .name(tableName)));
            return Optional.of(GlueConverter.fromGlueTable(result.table(), databaseName));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = toGlueTableInput(table);
            stats.getCreateTable().call(() -> glueClient.createTable(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(table.getDatabaseName())
                    .tableInput(input)));
        }
        catch (AlreadyExistsException e) {
            // Do not throw TableAlreadyExistsException if this query has already created the table.
            // This may happen when an actually successful metastore create call is retried
            // because of a timeout on our side.
            String expectedQueryId = table.getParameters().get(TRINO_QUERY_ID_NAME);
            if (expectedQueryId != null) {
                String existingQueryId = getTable(table.getDatabaseName(), table.getTableName())
                        .map(Table::getParameters)
                        .map(parameters -> parameters.get(TRINO_QUERY_ID_NAME))
                        .orElse(null);
                if (expectedQueryId.equals(existingQueryId)) {
                    return;
                }
            }
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDatabaseName(), table.getTableName()), e);
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(table.getDatabaseName(), e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateTable(table.getDatabaseName(), table.getTableName(), true);
            glueCache.invalidateTables(table.getDatabaseName());
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Optional<Location> deleteLocation = Optional.empty();
        if (deleteData) {
            Table table = getTable(databaseName, tableName)
                    .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
            if (table.getTableType().equals(MANAGED_TABLE.name())) {
                deleteLocation = table.getStorage()
                        .getOptionalLocation()
                        .filter(not(String::isEmpty))
                        .map(Location::of);
            }
        }

        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .applyMutation(glueContext::configureClient)
                .databaseName(databaseName)
                .name(tableName)
                .build();
        try {
            Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY)
                    .run(() -> stats.getDeleteTable().call(() -> glueClient.deleteTable(deleteTableRequest)));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateTable(databaseName, tableName, true);
            glueCache.invalidateTables(databaseName);
        }

        deleteLocation.ifPresent(this::deleteDir);
    }

    private void deleteDir(Location path)
    {
        try {
            fileSystem.deleteDirectory(path);
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

        updateTable(databaseName, tableName, ignored -> newTable);
    }

    private void updateTable(String databaseName, String tableName, TableModifier modifier)
    {
        try {
            Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY).run(() -> {
                Table existingTable = getTable(databaseName, tableName)
                        .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
                Table newTable = modifier.modify(existingTable);
                if (!existingTable.getDatabaseName().equals(newTable.getDatabaseName()) || !existingTable.getTableName().equals(newTable.getTableName())) {
                    throw new TrinoException(NOT_SUPPORTED, "Update cannot be used to change rename a table");
                }
                if (existingTable.getParameters().getOrDefault("table_type", "").equalsIgnoreCase("iceberg") && !Objects.equals(
                        existingTable.getParameters().get("metadata_location"),
                        newTable.getParameters().get("previous_metadata_location"))) {
                    throw new TrinoException(NOT_SUPPORTED, "Cannot update Iceberg table: supplied previous location does not match current location");
                }
                try {
                    stats.getUpdateTable().call(() -> glueClient.updateTable(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableInput(toGlueTableInput(newTable))));
                }
                catch (EntityNotFoundException e) {
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                }
            });
        }
        catch (FailsafeException e) {
            throwIfUnchecked(e.getCause());
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateTable(databaseName, tableName, false);
        }
    }

    private interface TableModifier
    {
        Table modify(Table table)
                throws Exception;
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        // read the existing table
        software.amazon.awssdk.services.glue.model.Table table;
        try {
            table = stats.getGetTable().call(() -> glueClient.getTable(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .name(tableName))
                    .table());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        // create a new table with the same data as the old table
        try {
            CreateTableRequest createTableRequest = CreateTableRequest.builder()
                    .applyMutation(glueContext::configureClient)
                    .databaseName(newDatabaseName)
                    .tableInput(asTableInputBuilder(table)
                            .name(newTableName)
                            .build())
                    .build();
            stats.getCreateTable().call(() -> glueClient.createTable(createTableRequest));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(databaseName, tableName));
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        // drop the old table
        try {
            dropTable(databaseName, tableName, false);
        }
        catch (RuntimeException e) {
            // if the drop failed, try to clean up the new table
            try {
                dropTable(databaseName, tableName, false);
            }
            catch (RuntimeException cleanupException) {
                if (!cleanupException.equals(e)) {
                    e.addSuppressed(cleanupException);
                }
            }
            throw e;
        }
        finally {
            glueCache.invalidateTable(databaseName, tableName, true);
            glueCache.invalidateTable(newDatabaseName, newTableName, true);
            glueCache.invalidateTables(databaseName);
            if (!databaseName.equals(newDatabaseName)) {
                glueCache.invalidateTables(newDatabaseName);
            }
        }
    }

    private static TableInput.Builder asTableInputBuilder(software.amazon.awssdk.services.glue.model.Table table)
    {
        return TableInput.builder()
                .name(table.name())
                .description(table.description())
                .owner(table.owner())
                .lastAccessTime(table.lastAccessTime())
                .lastAnalyzedTime(table.lastAnalyzedTime())
                .retention(table.retention())
                .storageDescriptor(table.storageDescriptor())
                .partitionKeys(table.partitionKeys())
                .viewOriginalText(table.viewOriginalText())
                .viewExpandedText(table.viewExpandedText())
                .tableType(table.tableType())
                .targetTable(table.targetTable())
                .parameters(table.parameters());
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        updateTable(databaseName, tableName, table -> table.withComment(comment));
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        updateTable(databaseName, tableName, table -> table.withOwner(principal));
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return glueCache.getTableColumnStatistics(
                databaseName,
                tableName,
                columnNames,
                missingColumnNames -> getTableColumnStatisticsInternal(databaseName, tableName, missingColumnNames));
    }

    private Map<String, HiveColumnStatistics> getTableColumnStatisticsInternal(String databaseName, String tableName, Set<String> columnNames)
    {
        var columnStatsTasks = Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE).stream()
                .map(partialColumns -> (Callable<List<ColumnStatistics>>) () -> stats.getGetColumnStatisticsForTable().call(() -> glueClient.getColumnStatisticsForTable(builder -> builder
                                .applyMutation(glueContext::configureClient)
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .columnNames(partialColumns))
                        .columnStatisticsList()))
                .collect(toImmutableList());

        try {
            return fromGlueStatistics(runParallel(columnStatsTasks));
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed fetching column statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
        }
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        verify(!transaction.isTransactional(), "Glue metastore does not support Hive Acid tables");

        // adding zero rows does not change stats
        if (mode == StatisticsUpdateMode.MERGE_INCREMENTAL && statisticsUpdate.getBasicStatistics().getRowCount().equals(OptionalLong.of(0))) {
            return;
        }

        // existing column statistics are required, for the columns being updated when in merge incremental mode
        // this is fetched before the basic statistics are updated, to avoid reloading in the case of retries updating basic statistics
        Map<String, HiveColumnStatistics> existingColumnStatistics;
        if (mode == StatisticsUpdateMode.MERGE_INCREMENTAL) {
            existingColumnStatistics = getTableColumnStatistics(databaseName, tableName, statisticsUpdate.getColumnStatistics().keySet());
        }
        else {
            existingColumnStatistics = ImmutableMap.of();
        }

        // first update the basic statistics on the table, which requires a read-modify-write cycle
        BasicTableStatisticsResult result;
        try {
            result = Failsafe.with(CONCURRENT_MODIFICATION_EXCEPTION_RETRY_POLICY)
                    .get(context -> updateBasicTableStatistics(databaseName, tableName, mode, statisticsUpdate, existingColumnStatistics));
        }
        catch (FailsafeException e) {
            throwIfUnchecked(e.getCause());
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateTable(databaseName, tableName, true);
        }

        List<Callable<Void>> tasks = new ArrayList<>();
        Lists.partition(toGlueColumnStatistics(result.updateColumnStatistics()), GLUE_COLUMN_WRITE_STAT_PAGE_SIZE).stream()
                .map(chunk -> (Callable<Void>) () -> {
                    stats.getUpdateColumnStatisticsForTable().call(() -> glueClient.updateColumnStatisticsForTable(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .columnStatisticsList(chunk)));
                    return null;
                })
                .forEach(tasks::add);

        result.removeColumnStatistics().stream()
                .map(columnName -> (Callable<Void>) () -> {
                    stats.getDeleteColumnStatisticsForTable().call(() -> glueClient.deleteColumnStatisticsForTable(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .columnName(columnName)));
                    return null;
                })
                .forEach(tasks::add);
        try {
            runParallel(tasks);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed updating column statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
        }
        finally {
            glueCache.invalidateTableColumnStatistics(databaseName, tableName);
        }
    }

    private BasicTableStatisticsResult updateBasicTableStatistics(
            String databaseName,
            String tableName,
            StatisticsUpdateMode mode,
            PartitionStatistics statisticsUpdate,
            Map<String, HiveColumnStatistics> existingColumnStatistics)
    {
        Table table = getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));

        PartitionStatistics existingTableStats = new PartitionStatistics(getHiveBasicStatistics(table.getParameters()), existingColumnStatistics);
        PartitionStatistics updatedStatistics = mode.updatePartitionStatistics(existingTableStats, statisticsUpdate);

        stats.getUpdateTable().call(() -> glueClient.updateTable(builder -> builder
                .applyMutation(glueContext::configureClient)
                .databaseName(databaseName)
                .tableInput(toGlueTableInput(table.withParameters(updateStatisticsParameters(table.getParameters(), updatedStatistics.getBasicStatistics()))))));

        Map<String, Column> columns = Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream())
                .collect(toImmutableMap(Column::getName, identity()));

        Map<Column, HiveColumnStatistics> updateColumnStatistics = columns.values().stream()
                .filter(column -> updatedStatistics.getColumnStatistics().containsKey(column.getName()))
                .collect(toImmutableMap(identity(), column -> updatedStatistics.getColumnStatistics().get(column.getName())));

        Set<String> removeColumnStatistics = switch (mode) {
            case OVERWRITE_SOME_COLUMNS -> ImmutableSet.of();
            case OVERWRITE_ALL, MERGE_INCREMENTAL -> columns.entrySet().stream()
                    // all columns not being updated are removed
                    .filter(entry -> !updateColumnStatistics.containsKey(entry.getValue()))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
            case UNDO_MERGE_INCREMENTAL, CLEAR_ALL -> columns.keySet();
        };

        return new BasicTableStatisticsResult(updateColumnStatistics, removeColumnStatistics);
    }

    private record BasicTableStatisticsResult(Map<Column, HiveColumnStatistics> updateColumnStatistics, Set<String> removeColumnStatistics) {}

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Column newColumn = new Column(columnName, columnType, Optional.ofNullable(columnComment), ImmutableMap.of());
        updateTable(databaseName, tableName, table -> table.withAddColumn(newColumn));
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        updateTable(databaseName, tableName, table -> table.withRenameColumn(oldColumnName, newColumnName));
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
            throws EntityNotFoundException
    {
        updateTable(databaseName, tableName, table -> table.withDropColumn(columnName));
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
            throws EntityNotFoundException
    {
        updateTable(databaseName, tableName, table -> table.withColumnComment(columnName, comment));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        if (partitionKeysFilter.isNone()) {
            return Optional.of(ImmutableList.of());
        }
        String glueFilterExpression = GlueExpressionUtil.buildGlueExpression(columnNames, partitionKeysFilter, assumeCanonicalPartitionKeys);
        Set<PartitionName> partitionNames = glueCache.getPartitionNames(
                databaseName,
                tableName,
                glueFilterExpression,
                cachePartition -> getPartitionNames(cachePartition, databaseName, tableName, glueFilterExpression));

        return Optional.of(partitionNames.stream()
                .map(PartitionName::partitionValues)
                .map(partitionValues -> toPartitionName(columnNames, partitionValues))
                .collect(toImmutableList()));
    }

    private Set<PartitionName> getPartitionNames(Consumer<Partition> cachePartition, String databaseName, String tableName, String glueExpression)
            throws EntityNotFoundException
    {
        if (partitionSegments == 1) {
            return getPartitionNames(cachePartition, databaseName, tableName, glueExpression, null);
        }

        try {
            return runParallel(IntStream.range(0, partitionSegments)
                    .mapToObj(segmentNumber -> (Callable<Set<PartitionName>>) () -> getPartitionNames(
                            cachePartition,
                            databaseName,
                            tableName,
                            glueExpression,
                            Segment.builder()
                                    .segmentNumber(segmentNumber)
                                    .totalSegments(partitionSegments)
                                    .build()))
                    .toList())
                    .stream()
                    .flatMap(Collection::stream)
                    .collect(toImmutableSet());
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, e.getCause());
        }
    }

    private Set<PartitionName> getPartitionNames(Consumer<Partition> cachePartition, String databaseName, String tableName, String expression, @Nullable Segment segment)
            throws EntityNotFoundException
    {
        try {
            return stats.getGetPartitionNames().call(() -> glueClient.getPartitionsPaginator(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .expression(expression)
                            .segment(segment)
                            .maxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS)).stream()
                    .map(GetPartitionsResponse::partitions)
                    .flatMap(List::stream)
                    .map(partition -> GlueConverter.fromGluePartition(databaseName, tableName, partition))
                    .peek(cachePartition)
                    .map(Partition::getValues)
                    .map(PartitionName::new)
                    .collect(toImmutableSet()));
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        String databaseName = table.getDatabaseName();
        String tableName = table.getTableName();
        PartitionName partitionName = new PartitionName(partitionValues);
        return glueCache.getPartition(databaseName, tableName, partitionName, () -> getPartition(databaseName, tableName, partitionName));
    }

    private Optional<Partition> getPartition(String databaseName, String tableName, PartitionName partitionName)
    {
        try {
            GetPartitionResponse result = stats.getGetPartition().call(() -> glueClient.getPartition(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(partitionName.partitionValues())));
            return Optional.of(GlueConverter.fromGluePartition(databaseName, tableName, result.partition()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        List<PartitionName> names = partitionNames.stream()
                .map(HivePartitionManager::extractPartitionValues)
                .map(PartitionName::new)
                .collect(toImmutableList());
        return stats.getGetPartitionByName().call(() -> getPartitionsByNames(table.getDatabaseName(), table.getTableName(), names)).entrySet().stream()
                .collect(toImmutableMap(entry -> makePartitionName(table.getPartitionColumns(), entry.getKey().partitionValues()), Entry::getValue));
    }

    private Map<PartitionName, Optional<Partition>> getPartitionsByNames(String databaseName, String tableName, Collection<PartitionName> partitionNames)
    {
        Collection<Partition> partitions = glueCache.batchGetPartitions(
                databaseName,
                tableName,
                partitionNames,
                (cachePartition, missingPartitions) -> batchGetPartition(databaseName, tableName, missingPartitions, cachePartition));
        Map<PartitionName, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(partition -> new PartitionName(partition.getValues()), identity()));

        ImmutableMap.Builder<PartitionName, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (PartitionName partitionName : partitionNames) {
            Partition partition = partitionValuesToPartitionMap.get(partitionName);
            resultBuilder.put(partitionName, Optional.ofNullable(partition));
        }
        return resultBuilder.buildOrThrow();
    }

    private Collection<Partition> batchGetPartition(String databaseName, String tableName, Collection<PartitionName> partitionNames, Consumer<Partition> cachePartition)
            throws EntityNotFoundException
    {
        try {
            List<PartitionValueList> pendingPartitions = partitionNames.stream()
                    .map(partitionName -> PartitionValueList.builder().values(partitionName.partitionValues()).build())
                    .collect(toCollection(ArrayList::new));

            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());
            while (!pendingPartitions.isEmpty()) {
                List<BatchGetPartitionResponse> responses = runParallel(Lists.partition(pendingPartitions, BATCH_GET_PARTITION_MAX_PAGE_SIZE).stream()
                        .map(partitions -> (Callable<BatchGetPartitionResponse>) () -> stats.getGetPartitions().call(() -> glueClient.batchGetPartition(builder -> builder
                                .applyMutation(glueContext::configureClient)
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .partitionsToGet(partitions))))
                        .toList());
                pendingPartitions.clear();

                for (var response : responses) {
                    // In the unlikely scenario where batchGetPartition call cannot make progress on retrieving partitions, avoid infinite loop.
                    // We fail only in case there are still unprocessedKeys. Case with empty partitions and empty unprocessedKeys is correct in case partitions from request are not found.
                    if (response.partitions().isEmpty() && !response.unprocessedKeys().isEmpty()) {
                        throw new TrinoException(HIVE_METASTORE_ERROR, "Cannot make progress retrieving partitions. Unable to retrieve partitions: " + response.unprocessedKeys());
                    }

                    response.partitions().stream()
                            .map(partition -> GlueConverter.fromGluePartition(databaseName, tableName, partition))
                            .peek(cachePartition)
                            .forEach(resultsBuilder::add);
                    pendingPartitions.addAll(response.unprocessedKeys());
                }
            }
            return resultsBuilder.build();
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed fetching partitions for %s.%s".formatted(databaseName, tableName), e.getCause());
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitionsWithStatistics)
    {
        if (partitionsWithStatistics.isEmpty()) {
            return;
        }
        partitionsWithStatistics.stream()
                .map(PartitionWithStatistics::getPartition)
                .collect(toImmutableList())
                .forEach(partition -> {
                    if (!partition.getDatabaseName().equals(databaseName) ||
                            !partition.getTableName().equals(tableName)) {
                        throw new TrinoException(NOT_SUPPORTED, "All partitions must belong to the same table");
                    }
                });

        List<Partition> updatedPartitions = partitionsWithStatistics.stream()
                .map(partitionWithStatistics -> {
                    Partition partition = partitionWithStatistics.getPartition();
                    HiveBasicStatistics basicStatistics = partitionWithStatistics.getStatistics().getBasicStatistics();
                    return partition.withParameters(updateStatisticsParameters(partition.getParameters(), basicStatistics));
                })
                .toList();
        var createPartitionTasks = Lists.partition(updatedPartitions, BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE).stream()
                .map(partitionBatch -> (Callable<Void>) () -> {
                    stats.getCreatePartitions().call(() -> glueClient.batchCreatePartition(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .partitionInputList(partitionBatch.stream()
                                    .map(GlueConverter::toGluePartitionInput)
                                    .collect(toImmutableList()))));
                    return null;
                })
                .toList();
        try {
            runParallel(createPartitionTasks);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof AlreadyExistsException) {
                throw new TrinoException(ALREADY_EXISTS, "Partition already exists in table %s.%s: %s".formatted(databaseName, tableName, e.getCause().getMessage()));
            }
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed creating partitions for %s.%s: %s".formatted(databaseName, tableName, e.getCause().getMessage()));
        }

        // statistics are created after partitions because it is not clear if ordering matters in Glue
        var createStatisticsTasks = partitionsWithStatistics.stream()
                .map(partitionWithStatistics -> createUpdatePartitionStatisticsTasks(
                        StatisticsUpdateMode.OVERWRITE_ALL,
                        partitionWithStatistics.getPartition(),
                        partitionWithStatistics.getStatistics().getColumnStatistics()))
                .flatMap(Collection::stream)
                .collect(toImmutableList());
        try {
            runParallel(createStatisticsTasks);
        }
        catch (ExecutionException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed creating partition column statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        PartitionName partitionName = new PartitionName(partitionValues);
        Optional<Location> location = Optional.empty();
        if (deleteData) {
            Partition partition = getPartition(databaseName, tableName, partitionName)
                    .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues));
            location = Optional.of(Strings.nullToEmpty(partition.getStorage().getLocation()))
                    .map(Location::of);
        }
        try {
            stats.getDeletePartition().call(() -> glueClient.deletePartition(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(partitionValues)));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partitionValues);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidatePartition(databaseName, tableName, partitionName);
        }

        location.ifPresent(this::deleteDir);
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        if (!partitionWithStatistics.getPartition().getDatabaseName().equals(databaseName) || !partitionWithStatistics.getPartition().getTableName().equals(tableName)) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Partition names must match table name");
        }

        alterPartition(partitionWithStatistics);
    }

    private void alterPartition(PartitionWithStatistics partitionWithStatistics)
    {
        Partition partition = partitionWithStatistics.getPartition();
        try {
            HiveBasicStatistics basicStatistics = partitionWithStatistics.getStatistics().getBasicStatistics();
            PartitionInput newPartition = toGluePartitionInput(partition.withParameters(updateStatisticsParameters(partition.getParameters(), basicStatistics)));
            stats.getUpdatePartition().call(() -> glueClient.updatePartition(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(partition.getDatabaseName())
                    .tableName(partition.getTableName())
                    .partitionInput(newPartition)
                    .partitionValueList(partition.getValues())));
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(partition.getSchemaTableName(), partition.getValues());
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidatePartition(partition.getDatabaseName(), partition.getTableName(), new PartitionName(partition.getValues()));
        }

        try {
            runParallel(createUpdatePartitionStatisticsTasks(StatisticsUpdateMode.OVERWRITE_ALL, partition, partitionWithStatistics.getStatistics().getColumnStatistics()));
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                throw new PartitionNotFoundException(new SchemaTableName(partition.getDatabaseName(), partition.getTableName()), partition.getValues());
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed updating partition column statistics for %s %s".formatted(partition.getSchemaTableName(), partition.getValues()), e.getCause());
        }
        finally {
            // this redundant, but be safe in case stats were cached before the update
            glueCache.invalidatePartition(partition.getDatabaseName(), partition.getTableName(), new PartitionName(partition.getValues()));
        }
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(
            String databaseName,
            String tableName,
            Set<String> partitionNames,
            Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        Map<PartitionName, String> partitionsNameMap = partitionNames.stream()
                .collect(toImmutableMap(partitionName -> new PartitionName(HivePartitionManager.extractPartitionValues(partitionName)), identity()));
        return getPartitionColumnStatistics(databaseName, tableName, partitionsNameMap.keySet(), columnNames).entrySet().stream()
                .collect(toImmutableMap(entry -> partitionsNameMap.get(entry.getKey()), Entry::getValue));
    }

    private Map<PartitionName, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Collection<PartitionName> partitionNames, Set<String> columnNames)
            throws EntityNotFoundException
    {
        Map<PartitionName, Map<String, HiveColumnStatistics>> columnStats;
        try {
            columnStats = runParallel(partitionNames.stream()
                    .map(partitionName -> (Callable<Entry<PartitionName, Map<String, HiveColumnStatistics>>>) () -> entry(
                            partitionName,
                            glueCache.getPartitionColumnStatistics(
                                    databaseName,
                                    tableName,
                                    partitionName,
                                    columnNames,
                                    missingColumnNames -> getPartitionColumnStatisticsInternal(databaseName, tableName, partitionName, missingColumnNames))))
                    .collect(toImmutableList()))
                    .stream()
                    .collect(toImmutableMap(Entry::getKey, Entry::getValue));
        }
        catch (ExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed fetching partition statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
        }

        // use empty statistics for missing partitions
        return partitionNames.stream()
                .collect(toImmutableMap(identity(), name -> columnStats.getOrDefault(name, ImmutableMap.of())));
    }

    private Map<String, HiveColumnStatistics> getPartitionColumnStatisticsInternal(String databaseName, String tableName, PartitionName partitionName, Set<String> columnNames)
            throws EntityNotFoundException
    {
        var columnStatsTasks = Lists.partition(ImmutableList.copyOf(columnNames), GLUE_COLUMN_READ_STAT_PAGE_SIZE).stream()
                .map(partialColumns -> (Callable<List<ColumnStatistics>>) () -> stats.getGetColumnStatisticsForPartition().call(() -> glueClient.getColumnStatisticsForPartition(builder -> builder
                                .applyMutation(glueContext::configureClient)
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .partitionValues(partitionName.partitionValues())
                                .columnNames(partialColumns))
                        .columnStatisticsList()))
                .collect(toImmutableList());

        try {
            var glueColumnStatistics = runParallel(columnStatsTasks);
            return fromGlueStatistics(glueColumnStatistics);
        }
        catch (ExecutionException e) {
            if (e.getCause() instanceof EntityNotFoundException) {
                return ImmutableMap.of();
            }
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed fetching column statistics for %s.%s %s".formatted(databaseName, tableName, partitionName), e.getCause());
        }
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        batchUpdatePartitionStatistics(table.getDatabaseName(), table.getTableName(), mode, partitionUpdates.entrySet().stream()
                // filter out empty statistics for merge incremental mode
                .filter(entry -> mode != StatisticsUpdateMode.MERGE_INCREMENTAL || !entry.getValue().getBasicStatistics().getRowCount().equals(OptionalLong.of(0)))
                .collect(toImmutableMap(entry -> new PartitionName(HivePartitionManager.extractPartitionValues(entry.getKey())), Entry::getValue)));
    }

    private void batchUpdatePartitionStatistics(String databaseName, String tableName, StatisticsUpdateMode mode, Map<PartitionName, PartitionStatistics> newStatistics)
            throws EntityNotFoundException
    {
        // partitions are required for update
        // the glue cache is not used here because we need up-to-date partitions for the read-update-write operation
        // loaded partitions are not cached here since the partition values are invalidated after the update
        Collection<Partition> partitions = batchGetPartition(databaseName, tableName, ImmutableList.copyOf(newStatistics.keySet()), ignored -> {});

        // existing column statistics are required for the columns being updated when in merge incremental mode
        // this is fetched before the basic statistics are updated, to avoid reloading in the case of retries updating basic statistics
        Map<PartitionName, Map<String, HiveColumnStatistics>> existingColumnStatistics;
        if (mode == StatisticsUpdateMode.MERGE_INCREMENTAL) {
            try {
                existingColumnStatistics = runParallel(newStatistics.entrySet().stream()
                        .map(entry -> (Callable<Entry<PartitionName, Map<String, HiveColumnStatistics>>>) () -> {
                            PartitionName partitionName = entry.getKey();
                            Map<String, HiveColumnStatistics> columnStatistics = getPartitionColumnStatisticsInternal(databaseName, tableName, partitionName, entry.getValue().getColumnStatistics().keySet());
                            return entry(partitionName, columnStatistics);
                        })
                        .collect(toImmutableList()))
                        .stream()
                        .collect(toImmutableMap(Entry::getKey, Entry::getValue));
            }
            catch (ExecutionException e) {
                throwIfUnchecked(e.getCause());
                throw new TrinoException(HIVE_METASTORE_ERROR, "Failed fetching current partition statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
            }
        }
        else {
            existingColumnStatistics = ImmutableMap.of();
        }

        List<PartitionStatisticsUpdate> partitionStatisticsUpdates = new ArrayList<>();
        for (Partition partition : partitions) {
            PartitionName partitionName = new PartitionName(partition.getValues());
            PartitionStatistics existingStats = new PartitionStatistics(
                    getHiveBasicStatistics(partition.getParameters()),
                    existingColumnStatistics.getOrDefault(partitionName, ImmutableMap.of()));
            PartitionStatistics newStats = newStatistics.get(partitionName);
            PartitionStatistics updatedStatistics = mode.updatePartitionStatistics(existingStats, newStats);
            Partition updatedPartition = partition.withParameters(updateStatisticsParameters(partition.getParameters(), updatedStatistics.getBasicStatistics()));
            boolean basicStatsUpdated = !updatedStatistics.getBasicStatistics().equals(existingStats.getBasicStatistics());
            partitionStatisticsUpdates.add(new PartitionStatisticsUpdate(updatedPartition, basicStatsUpdated, updatedStatistics.getColumnStatistics()));
        }

        // partitions and statistics are updated in concurrently
        List<Callable<Void>> tasks = new ArrayList<>();
        partitionStatisticsUpdates.stream()
                .map(update -> createUpdatePartitionStatisticsTasks(mode, update.updatedPartition(), update.updatedStatistics()))
                .flatMap(Collection::stream)
                .forEach(tasks::add);

        List<Partition> updatedPartitions = partitionStatisticsUpdates.stream()
                .filter(PartitionStatisticsUpdate::basicStatsUpdated)
                .map(PartitionStatisticsUpdate::updatedPartition)
                .collect(toImmutableList());
        Lists.partition(updatedPartitions, BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE).stream()
                .map(partitionBatch -> (Callable<Void>) () -> {
                    stats.getBatchUpdatePartition().call(() -> glueClient.batchUpdatePartition(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .entries(partitionBatch.stream()
                                    .map(partition -> BatchUpdatePartitionRequestEntry.builder()
                                            .partitionValueList(partition.getValues())
                                            .partitionInput(toGluePartitionInput(partition))
                                            .build())
                                    .collect(toImmutableList()))));
                    return null;
                })
                .forEach(tasks::add);

        try {
            runParallel(tasks);
        }
        catch (ExecutionException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed updating partition column statistics for %s.%s".formatted(databaseName, tableName), e.getCause());
        }
    }

    private List<Callable<Void>> createUpdatePartitionStatisticsTasks(StatisticsUpdateMode mode, Partition partition, Map<String, HiveColumnStatistics> updatedStatistics)
    {
        Map<String, Column> columns = partition.getColumns().stream()
                .collect(toImmutableMap(Column::getName, identity()));

        Map<Column, HiveColumnStatistics> statisticsByColumn = columns.values().stream()
                .filter(column -> updatedStatistics.containsKey(column.getName()))
                .collect(toImmutableMap(identity(), column -> updatedStatistics.get(column.getName())));

        Set<String> removeColumnStatistics = switch (mode) {
            case OVERWRITE_SOME_COLUMNS -> ImmutableSet.of();
            case OVERWRITE_ALL, MERGE_INCREMENTAL -> columns.entrySet().stream()
                    // all columns not being updated are removed
                    .filter(entry -> !statisticsByColumn.containsKey(entry.getValue()))
                    .map(Entry::getKey)
                    .collect(Collectors.toSet());
            case UNDO_MERGE_INCREMENTAL, CLEAR_ALL -> columns.keySet();
        };

        ImmutableList.Builder<Callable<Void>> tasks = ImmutableList.builder();
        Lists.partition(toGlueColumnStatistics(statisticsByColumn), GLUE_COLUMN_WRITE_STAT_PAGE_SIZE).stream()
                .map(chunk -> (Callable<Void>) () -> {
                    stats.getUpdateColumnStatisticsForPartition().call(() -> glueClient.updateColumnStatisticsForPartition(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(partition.getDatabaseName())
                            .tableName(partition.getTableName())
                            .partitionValues(partition.getValues())
                            .columnStatisticsList(chunk)));
                    return null;
                })
                .forEach(tasks::add);
        removeColumnStatistics.stream()
                .map(columnName -> (Callable<Void>) () -> {
                    stats.getDeleteColumnStatisticsForPartition().call(() -> glueClient.deleteColumnStatisticsForPartition(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(partition.getDatabaseName())
                            .tableName(partition.getTableName())
                            .partitionValues(partition.getValues())
                            .columnName(columnName)));
                    return null;
                })
                .forEach(tasks::add);
        return tasks.build();
    }

    private record PartitionStatisticsUpdate(Partition updatedPartition, boolean basicStatsUpdated, Map<String, HiveColumnStatistics> updatedStatistics) {}

    //
    // Function management
    //

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        try {
            // this call is only used for create/drop function so is not worth caching
            stats.getUpdateUserDefinedFunction().call(() -> glueClient.getUserDefinedFunction(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, signatureToken))));
            return true;
        }
        catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException e) {
            return false;
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return glueCache.getAllFunctions(databaseName, () -> getFunctionsByPatternInternal(databaseName, "trino__.*"));
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return glueCache.getFunction(databaseName, functionName, () -> getFunctionsByPatternInternal(databaseName, "trino__" + Pattern.quote(functionName) + "__.*"));
    }

    private Collection<LanguageFunction> getFunctionsByPatternInternal(String databaseName, String functionNamePattern)
    {
        try {
            return stats.getGetUserDefinedFunctions().call(() -> glueClient.getUserDefinedFunctionsPaginator(builder -> builder
                            .applyMutation(glueContext::configureClient)
                            .databaseName(databaseName)
                            .pattern(functionNamePattern)
                            .maxResults(AWS_GLUE_GET_FUNCTIONS_MAX_RESULTS)).stream()
                    .map(GetUserDefinedFunctionsResponse::userDefinedFunctions)
                    .flatMap(List::stream)
                    .map(GlueConverter::fromGlueFunction)
                    .collect(toImmutableList()));
        }
        catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException | AccessDeniedException e) {
            return ImmutableList.of();
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        if (functionName.contains("__")) {
            throw new TrinoException(NOT_SUPPORTED, "Function names with double underscore are not supported");
        }
        try {
            software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput functionInput = toGlueFunctionInput(functionName, function);
            stats.getCreateUserDefinedFunction().call(() -> glueClient.createUserDefinedFunction(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .functionInput(functionInput)));
        }
        catch (software.amazon.awssdk.services.glue.model.AlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Function already exists: %s.%s".formatted(databaseName, functionName), e);
        }
        catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateFunction(databaseName, functionName);
        }
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        try {
            UserDefinedFunctionInput functionInput = toGlueFunctionInput(functionName, function);
            stats.getUpdateUserDefinedFunction().call(() -> glueClient.updateUserDefinedFunction(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, function.signatureToken()))
                    .functionInput(functionInput)));
        }
        catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException e) {
            throw new TrinoException(FUNCTION_NOT_FOUND, "Function not found: %s.%s".formatted(databaseName, functionName), e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateFunction(databaseName, functionName);
        }
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        try {
            stats.getDeleteUserDefinedFunction().call(() -> glueClient.deleteUserDefinedFunction(builder -> builder
                    .applyMutation(glueContext::configureClient)
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, signatureToken))));
        }
        catch (software.amazon.awssdk.services.glue.model.EntityNotFoundException e) {
            throw new TrinoException(FUNCTION_NOT_FOUND, "Function not found: %s.%s".formatted(databaseName, functionName), e);
        }
        catch (SdkException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            glueCache.invalidateFunction(databaseName, functionName);
        }
    }

    //
    // Glue does not support permissions management or ACID transactions
    //

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
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        if (principal.getType() == USER) {
            return ImmutableSet.of(new RoleGrant(principal.toTrinoPrincipal(), PUBLIC_ROLE_NAME, false));
        }
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "grantTablePrivileges is not supported by Glue");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
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

    /**
     * Run all tasks on executor returning as soon as all complete or any task fails.
     * Upon task execution failure, other tasks are canceled and interrupted, but not waited
     * for.
     *
     * @return results of all tasks in any order
     * @throws ExecutionException if any task fails; exception cause is the first task failure
     */
    private <T> List<T> runParallel(Collection<Callable<T>> tasks)
            throws ExecutionException
    {
        CompletionService<T> completionService = new ExecutorCompletionService<>(executor);
        List<Future<T>> futures = new ArrayList<>(tasks.size());
        for (Callable<T> task : tasks) {
            futures.add(completionService.submit(task));
        }
        try {
            for (int i = 0; i < futures.size(); i++) {
                completionService.take();
            }

            List<T> results = new ArrayList<>(futures.size());
            for (Future<T> future : futures) {
                results.add(future.get());
            }
            return Collections.unmodifiableList(results);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TrinoException(HIVE_METASTORE_ERROR, "Interrupted", e);
        }
        finally {
            futures.forEach(future -> future.cancel(true));
        }
    }

    public enum TableKind
    {
        OTHER, DELTA, ICEBERG, HUDI
    }

    private static Predicate<software.amazon.awssdk.services.glue.model.Table> createTablePredicate(Set<TableKind> visibleTableKinds)
    {
        // nothing is visible
        if (visibleTableKinds.isEmpty()) {
            return table -> false;
        }

        // everything is visible
        if (visibleTableKinds.equals(ImmutableSet.copyOf(TableKind.values()))) {
            return table -> true;
        }

        // exclusion
        if (visibleTableKinds.contains(TableKind.OTHER)) {
            EnumSet<TableKind> deniedKinds = EnumSet.complementOf(EnumSet.copyOf(visibleTableKinds));
            Predicate<software.amazon.awssdk.services.glue.model.Table> tableVisibilityFilter = table -> true;
            for (TableKind value : deniedKinds) {
                tableVisibilityFilter = tableVisibilityFilter.and(not(tableKindPredicate(value)));
            }
            return tableVisibilityFilter;
        }

        // inclusion
        Predicate<software.amazon.awssdk.services.glue.model.Table> tableVisibilityFilter = table -> false;
        for (TableKind tableKind : visibleTableKinds) {
            tableVisibilityFilter = tableVisibilityFilter.or(tableKindPredicate(tableKind));
        }
        return tableVisibilityFilter;
    }

    private static Predicate<software.amazon.awssdk.services.glue.model.Table> tableKindPredicate(TableKind tableKind)
    {
        return switch (tableKind) {
            case DELTA -> table -> isDeltaLakeTable(table.parameters());
            case ICEBERG -> table -> isIcebergTable(table.parameters());
            case HUDI -> table -> table.storageDescriptor() != null && isHudiTable(table.storageDescriptor().inputFormat());
            case OTHER -> throw new IllegalArgumentException("Predicate can not be created for OTHER");
        };
    }
}
