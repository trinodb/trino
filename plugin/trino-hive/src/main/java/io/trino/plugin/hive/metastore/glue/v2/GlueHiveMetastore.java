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
package io.trino.plugin.hive.metastore.glue.v2;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.airlift.concurrent.MoreFutures;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveBasicStatistics;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.metastore.glue.GlueExpressionUtil;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter;
import io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.GluePartitionConverter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;
import jakarta.annotation.Nullable;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.services.glue.GlueAsyncClient;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.glue.model.AccessDeniedException;
import software.amazon.awssdk.services.glue.model.AlreadyExistsException;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchCreatePartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchGetPartitionResponse;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionRequestEntry;
import software.amazon.awssdk.services.glue.model.BatchUpdatePartitionResponse;
import software.amazon.awssdk.services.glue.model.CreateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.CreateTableRequest;
import software.amazon.awssdk.services.glue.model.CreateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.DatabaseInput;
import software.amazon.awssdk.services.glue.model.DeleteDatabaseRequest;
import software.amazon.awssdk.services.glue.model.DeletePartitionRequest;
import software.amazon.awssdk.services.glue.model.DeleteTableRequest;
import software.amazon.awssdk.services.glue.model.DeleteUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.EntityNotFoundException;
import software.amazon.awssdk.services.glue.model.ErrorDetail;
import software.amazon.awssdk.services.glue.model.GetDatabaseRequest;
import software.amazon.awssdk.services.glue.model.GetDatabaseResponse;
import software.amazon.awssdk.services.glue.model.GetDatabasesRequest;
import software.amazon.awssdk.services.glue.model.GetDatabasesResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionResponse;
import software.amazon.awssdk.services.glue.model.GetPartitionsRequest;
import software.amazon.awssdk.services.glue.model.GetPartitionsResponse;
import software.amazon.awssdk.services.glue.model.GetTableRequest;
import software.amazon.awssdk.services.glue.model.GetTableResponse;
import software.amazon.awssdk.services.glue.model.GetTablesRequest;
import software.amazon.awssdk.services.glue.model.GetTablesResponse;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsRequest;
import software.amazon.awssdk.services.glue.model.GetUserDefinedFunctionsResponse;
import software.amazon.awssdk.services.glue.model.PartitionError;
import software.amazon.awssdk.services.glue.model.PartitionInput;
import software.amazon.awssdk.services.glue.model.PartitionValueList;
import software.amazon.awssdk.services.glue.model.Segment;
import software.amazon.awssdk.services.glue.model.TableInput;
import software.amazon.awssdk.services.glue.model.UpdateDatabaseRequest;
import software.amazon.awssdk.services.glue.model.UpdatePartitionRequest;
import software.amazon.awssdk.services.glue.model.UpdateTableRequest;
import software.amazon.awssdk.services.glue.model.UpdateUserDefinedFunctionRequest;
import software.amazon.awssdk.services.glue.model.UserDefinedFunctionInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Comparators.lexicographical;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.metastore.Partition.toPartitionValues;
import static io.trino.metastore.Table.TABLE_COMMENT;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TRINO_QUERY_ID_NAME;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.MetastoreUtil.metastoreFunctionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.toPartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.trino.plugin.hive.metastore.glue.v2.AwsSdkUtil.getPaginatedResults;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter.convertFunction;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter.convertGlueTableToTableInput;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueInputConverter.convertPartition;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableParameters;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.getTableType;
import static io.trino.plugin.hive.metastore.glue.v2.converter.GlueToTrinoConverter.mappedCopy;
import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.USER;
import static java.util.Objects.requireNonNull;
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
    private static final int AWS_GLUE_GET_DATABASES_MAX_RESULTS = 100;
    private static final int AWS_GLUE_GET_FUNCTIONS_MAX_RESULTS = 100;
    private static final int AWS_GLUE_GET_TABLES_MAX_RESULTS = 100;
    private static final Comparator<Iterable<String>> PARTITION_VALUE_COMPARATOR = lexicographical(String.CASE_INSENSITIVE_ORDER);

    private final TrinoFileSystem fileSystem;
    private final GlueClient glueClient;
    private final GlueAsyncClient asyncGlueClient;
    private final Optional<String> defaultDir;
    private final int partitionSegments;
    private final Executor partitionsReadExecutor;
    private final GlueColumnStatisticsProvider columnStatisticsProvider;
    private final boolean assumeCanonicalPartitionKeys;
    private final Predicate<software.amazon.awssdk.services.glue.model.Table> tableFilter;

    @Inject
    public GlueHiveMetastore(
            TrinoFileSystemFactory fileSystemFactory,
            GlueHiveMetastoreConfig glueConfig,
            @ForGlueHiveMetastore Executor partitionsReadExecutor,
            GlueColumnStatisticsProviderFactory columnStatisticsProviderFactory,
            GlueClient glueClient,
            GlueAsyncClient asyncGlueClient,
            @ForGlueHiveMetastore Predicate<software.amazon.awssdk.services.glue.model.Table> tableFilter)
    {
        this.fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser(DEFAULT_METASTORE_USER));
        this.glueClient = requireNonNull(glueClient, "glueClient is null");
        this.asyncGlueClient = requireNonNull(asyncGlueClient, "asyncGlueClient is null");
        this.defaultDir = glueConfig.getDefaultWarehouseDir();
        this.partitionSegments = glueConfig.getPartitionSegments();
        this.partitionsReadExecutor = requireNonNull(partitionsReadExecutor, "partitionsReadExecutor is null");
        this.assumeCanonicalPartitionKeys = glueConfig.isAssumeCanonicalPartitionKeys();
        this.tableFilter = requireNonNull(tableFilter, "tableFilter is null");
        this.columnStatisticsProvider = columnStatisticsProviderFactory.createGlueColumnStatisticsProvider(glueClient);
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return getPaginatedResults(
                    request -> glueClient.getDatabases(request.build()),
                    GetDatabasesRequest.builder().maxResults(AWS_GLUE_GET_DATABASES_MAX_RESULTS),
                    GetDatabasesRequest.Builder::nextToken,
                    GetDatabasesResponse::nextToken)
                    .map(GetDatabasesResponse::databaseList)
                    .flatMap(List::stream)
                    .map(software.amazon.awssdk.services.glue.model.Database::name)
                    .collect(toImmutableList());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            GetDatabaseResponse result = glueClient.getDatabase(GetDatabaseRequest.builder().name(databaseName).build());
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
            DatabaseInput databaseInput = GlueInputConverter.convertDatabase(database);
            glueClient.createDatabase(CreateDatabaseRequest.builder().databaseInput(databaseInput).build());
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
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
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
        Optional<String> location = Optional.empty();
        if (deleteData) {
            location = getDatabase(databaseName)
                    .orElseThrow(() -> new SchemaNotFoundException(databaseName))
                    .getLocation();
        }

        try {
            glueClient.deleteDatabase(DeleteDatabaseRequest.builder().name(databaseName).build());
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        if (deleteData) {
            location.map(Location::of).ifPresent(this::deleteDir);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        try {
            Database database = getDatabase(databaseName).orElseThrow(() -> new SchemaNotFoundException(databaseName));
            DatabaseInput renamedDatabase = GlueInputConverter.convertDatabase(database).toBuilder().name(newDatabaseName).build();
            glueClient.updateDatabase(UpdateDatabaseRequest.builder()
                        .name(databaseName)
                        .databaseInput(renamedDatabase)
                        .build());
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
    public List<TableInfo> getTables(String databaseName)
    {
        try {
            return getGlueTables(databaseName)
                    .filter(tableFilter)
                    .map(table -> new TableInfo(
                            new SchemaTableName(databaseName, table.name()),
                            TableInfo.ExtendedRelationType.fromTableTypeAndComment(getTableType(table), getTableParameters(table).get(TABLE_COMMENT))))
                    .collect(toImmutableList());
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
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            GetTableResponse result = glueClient.getTable(GetTableRequest.builder()
                    .databaseName(databaseName)
                    .name(tableName)
                    .build());
            return Optional.of(GlueToTrinoConverter.convertTable(result.table(), databaseName));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private Table getExistingTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        try {
            TableInput input = GlueInputConverter.convertTable(table);
            glueClient.createTable(CreateTableRequest.builder()
                    .databaseName(table.getDatabaseName())
                    .tableInput(input)
                        .build());
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
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        Table table = getExistingTable(databaseName, tableName);
        DeleteTableRequest deleteTableRequest = DeleteTableRequest.builder()
                .databaseName(databaseName)
                .name(tableName)
                .build();
        try {
            glueClient.deleteTable(deleteTableRequest);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        Optional<String> location = table.getStorage().getOptionalLocation()
                .filter(not(String::isEmpty));
        if (deleteData && isManagedTable(table) && location.isPresent()) {
            deleteDir(Location.of(location.get()));
        }
    }

    private static boolean isManagedTable(Table table)
    {
        return table.getTableType().equals(MANAGED_TABLE.name());
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
        try {
            TableInput newTableInput = GlueInputConverter.convertTable(newTable);
            glueClient.updateTable(UpdateTableRequest.builder()
                        .databaseName(databaseName)
                        .tableInput(newTableInput)
                        .build());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), e);
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
                    .databaseName(databaseName)
                    .name(tableName)
                    .build();
            GetTableResponse glueTable = glueClient.getTable(getTableRequest);
            TableInput tableInput = convertGlueTableToTableInput(glueTable.table()).toBuilder().name(newTableName).build();
            CreateTableRequest createTableRequest = CreateTableRequest.builder()
                    .databaseName(newDatabaseName)
                    .tableInput(tableInput)
                    .build();
            glueClient.createTable(createTableRequest);
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

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        Table newTable = Table.builder(oldTable)
                .setParameter(TABLE_COMMENT, comment)
                .build();
        replaceTable(databaseName, tableName, newTable, null);
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
            TableInput newTableInput = GlueInputConverter.convertTable(table)
                    .toBuilder()
                    .owner(principal.getName())
                    .build();

            glueClient.updateTable(UpdateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(newTableInput)
                    .build());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), e);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        checkArgument(!columnNames.isEmpty(), "columnNames is empty");
        return columnStatisticsProvider.getTableColumnStatistics(databaseName, tableName, columnNames);
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        Table table = getExistingTable(databaseName, tableName);
        if (acidWriteId.isPresent()) {
            table = Table.builder(table).setWriteId(acidWriteId).build();
        }
        // load current statistics
        HiveBasicStatistics currentBasicStatistics = getHiveBasicStatistics(table.getParameters());
        Map<String, HiveColumnStatistics> currentColumnStatistics = getTableColumnStatistics(
                databaseName,
                tableName,
                Stream.concat(table.getDataColumns().stream(), table.getPartitionColumns().stream()).map(Column::getName).collect(toImmutableSet()));
        PartitionStatistics currentStatistics = new PartitionStatistics(currentBasicStatistics, currentColumnStatistics);

        PartitionStatistics updatedStatistics = mode.updatePartitionStatistics(currentStatistics, statisticsUpdate);

        try {
            final Map<String, String> statisticsParameters = updateStatisticsParameters(table.getParameters(), updatedStatistics.basicStatistics());
            TableInput tableInput = GlueInputConverter.convertTable(table)
                    .toBuilder()
                    .parameters(statisticsParameters)
                    .build();
            glueClient.updateTable(UpdateTableRequest.builder()
                    .databaseName(databaseName)
                    .tableInput(tableInput)
                    .build());
            columnStatisticsProvider.updateTableColumnStatistics(table, updatedStatistics.columnStatistics());
        }
        catch (EntityNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName), e);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        Table oldTable = getExistingTable(databaseName, tableName);
        Table newTable = Table.builder(oldTable)
                .addDataColumn(new Column(columnName, columnType, Optional.ofNullable(columnComment), ImmutableMap.of()))
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
                newDataColumns.add(new Column(newColumnName, column.getType(), column.getComment(), column.getProperties()));
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
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        Table table = getExistingTable(databaseName, tableName);
        List<Column> dataColumns = table.getDataColumns();
        List<Column> partitionColumns = table.getPartitionColumns();

        Optional<Integer> matchingDataColumn = indexOfColumnWithName(dataColumns, columnName);
        Optional<Integer> matchingPartitionColumn = indexOfColumnWithName(partitionColumns, columnName);

        if (matchingDataColumn.isPresent() && matchingPartitionColumn.isPresent()) {
            throw new TrinoException(HIVE_INVALID_METADATA, "Found two columns with names matching " + columnName);
        }
        if (matchingDataColumn.isEmpty() && matchingPartitionColumn.isEmpty()) {
            throw new ColumnNotFoundException(table.getSchemaTableName(), columnName);
        }

        Table updatedTable = Table.builder(table)
                .setDataColumns(matchingDataColumn.map(index -> setColumnCommentForIndex(dataColumns, index, comment)).orElse(dataColumns))
                .setPartitionColumns(matchingPartitionColumn.map(index -> setColumnCommentForIndex(partitionColumns, index, comment)).orElse(partitionColumns))
                .build();

        replaceTable(databaseName, tableName, updatedTable, null);
    }

    private static Optional<Integer> indexOfColumnWithName(List<Column> columns, String columnName)
    {
        Optional<Integer> index = Optional.empty();
        for (int i = 0; i < columns.size(); i++) {
            // Glue columns are always lowercase
            if (columns.get(i).getName().equals(columnName)) {
                index.ifPresent(_ -> {
                    throw new TrinoException(HIVE_INVALID_METADATA, "Found two columns with names matching " + columnName);
                });
                index = Optional.of(i);
            }
        }
        return index;
    }

    private static List<Column> setColumnCommentForIndex(List<Column> columns, int indexToUpdate, Optional<String> comment)
    {
        ImmutableList.Builder<Column> newColumns = ImmutableList.builder();
        for (int i = 0; i < columns.size(); i++) {
            Column originalColumn = columns.get(i);
            if (i == indexToUpdate) {
                newColumns.add(new Column(originalColumn.getName(), originalColumn.getType(), comment, originalColumn.getProperties()));
            }
            else {
                newColumns.add(originalColumn);
            }
        }
        return newColumns.build();
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

    private static List<String> buildPartitionNames(List<String> partitionColumns, List<List<String>> partitions)
    {
        return mappedCopy(partitions, partition -> toPartitionName(partitionColumns, partition));
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
            // Reuse immutable field instances opportunistically between partitions
            return getPaginatedResults(
                    builder -> glueClient.getPartitions(builder.build()),
                    GetPartitionsRequest.builder()
                            .databaseName(databaseName)
                            .tableName(tableName)
                            .expression(expression)
                            .segment(segment)
                            // We need the partition values, and not column schema which is very large
                            .excludeColumnSchema(true)
                            .maxResults(AWS_GLUE_GET_PARTITIONS_MAX_RESULTS),
                    GetPartitionsRequest.Builder::nextToken,
                    GetPartitionsResponse::nextToken)
                    .map(GetPartitionsResponse::partitions)
                    .flatMap(List::stream)
                    .map(software.amazon.awssdk.services.glue.model.Partition::values)
                    .collect(toImmutableList());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        try {
            GetPartitionResponse result = glueClient.getPartition(GetPartitionRequest.builder()
                    .databaseName(table.getDatabaseName())
                    .tableName(table.getTableName())
                    .partitionValues(partitionValues)
                    .build());
            return Optional.of(new GluePartitionConverter(table.getDatabaseName(), table.getTableName()).apply(result.partition()));
        }
        catch (EntityNotFoundException e) {
            return Optional.empty();
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    /**
     * <pre>
     * Ex: Partition keys = ['a', 'b']
     *     Partition names = ['a=1/b=2', 'a=2/b=2']
     * </pre>
     *
     * @param partitionNames List of full partition names
     * @return Mapping of partition name to the partition object
     */
    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return getPartitionsByNamesInternal(table, partitionNames);
    }

    private Map<String, Optional<Partition>> getPartitionsByNamesInternal(Table table, Collection<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        if (partitionNames.isEmpty()) {
            return ImmutableMap.of();
        }

        List<Partition> partitions = batchGetPartition(table, partitionNames);

        Map<String, List<String>> partitionNameToPartitionValuesMap = partitionNames.stream()
                .collect(toMap(identity(), Partition::toPartitionValues));
        Map<List<String>, Partition> partitionValuesToPartitionMap = partitions.stream()
                .collect(toMap(Partition::getValues, identity()));

        ImmutableMap.Builder<String, Optional<Partition>> resultBuilder = ImmutableMap.builder();
        for (Entry<String, List<String>> entry : partitionNameToPartitionValuesMap.entrySet()) {
            Partition partition = partitionValuesToPartitionMap.get(entry.getValue());
            resultBuilder.put(entry.getKey(), Optional.ofNullable(partition));
        }
        return resultBuilder.buildOrThrow();
    }

    private List<Partition> batchGetPartition(Table table, Collection<String> partitionNames)
    {
        List<Future<BatchGetPartitionResponse>> batchGetPartitionFutures = new ArrayList<>();
        try {
            List<PartitionValueList> pendingPartitions = partitionNames.stream()
                    .map(partitionName -> PartitionValueList.builder().values(toPartitionValues(partitionName)).build())
                    .collect(toCollection(ArrayList::new));

            ImmutableList.Builder<Partition> resultsBuilder = ImmutableList.builderWithExpectedSize(partitionNames.size());

            // Reuse immutable field instances opportunistically between partitions
            GluePartitionConverter converter = new GluePartitionConverter(table.getDatabaseName(), table.getTableName());

            while (!pendingPartitions.isEmpty()) {
                for (List<PartitionValueList> partitions : Lists.partition(pendingPartitions, BATCH_GET_PARTITION_MAX_PAGE_SIZE)) {
                    batchGetPartitionFutures.add(asyncGlueClient.batchGetPartition(BatchGetPartitionRequest.builder()
                                    .databaseName(table.getDatabaseName())
                                    .tableName(table.getTableName())
                                    .partitionsToGet(partitions)
                                    .build()));
                }
                pendingPartitions.clear();

                for (Future<BatchGetPartitionResponse> future : batchGetPartitionFutures) {
                    BatchGetPartitionResponse batchGetPartitionResult = future.get();
                    List<software.amazon.awssdk.services.glue.model.Partition> partitions = batchGetPartitionResult.partitions();
                    List<PartitionValueList> unprocessedKeys = batchGetPartitionResult.unprocessedKeys();

                    // In the unlikely scenario where batchGetPartition call cannot make progress on retrieving partitions, avoid infinite loop
                    // We fail only in case there are still unprocessedKeys. Case with empty partitions and empty unprocessedKeys is correct in case partitions from request are not found.
                    if (partitions.isEmpty() && !unprocessedKeys.isEmpty()) {
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
            List<Future<BatchCreatePartitionResponse>> futures = new ArrayList<>();

            for (List<PartitionWithStatistics> partitionBatch : Lists.partition(partitions, BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)) {
                List<PartitionInput> partitionInputs = mappedCopy(partitionBatch, GlueInputConverter::convertPartition);
                futures.add(asyncGlueClient.batchCreatePartition(
                        BatchCreatePartitionRequest
                                .builder()
                                .databaseName(databaseName)
                                .tableName(tableName)
                                .partitionInputList(partitionInputs)
                                .build()));
            }

            for (Future<BatchCreatePartitionResponse> future : futures) {
                try {
                    BatchCreatePartitionResponse result = future.get();
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
                            partitionWithStatistics.getStatistics().columnStatistics()))
                    .collect(toImmutableSet());
            columnStatisticsProvider.updatePartitionStatistics(updates);
        }
        catch (AwsServiceException | ExecutionException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private static void propagatePartitionErrorToTrinoException(String databaseName, String tableName, List<PartitionError> partitionErrors)
    {
        if (partitionErrors != null && !partitionErrors.isEmpty()) {
            ErrorDetail errorDetail = partitionErrors.getFirst().errorDetail();
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
            glueClient.deletePartition(DeletePartitionRequest.builder()
                    .databaseName(databaseName)
                    .tableName(tableName)
                    .partitionValues(parts)
                    .build());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }

        String partLocation = partition.getStorage().getLocation();
        if (deleteData && isManagedTable(table) && !isNullOrEmpty(partLocation)) {
            deleteDir(Location.of(partLocation));
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        try {
            PartitionInput newPartition = convertPartition(partition);

            glueClient.updatePartition(UpdatePartitionRequest.builder()
                        .databaseName(databaseName)
                        .tableName(tableName)
                        .partitionInput(newPartition)
                        .partitionValueList(partition.getPartition().getValues())
                        .build());
            columnStatisticsProvider.updatePartitionStatistics(
                    partition.getPartition(),
                    partition.getStatistics().columnStatistics());
        }
        catch (EntityNotFoundException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getPartition().getValues(), e);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
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
        return columnStatisticsProvider.getPartitionColumnStatistics(databaseName, tableName, partitionNames, columnNames);
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        Iterables.partition(partitionUpdates.entrySet(), BATCH_CREATE_PARTITION_MAX_PAGE_SIZE)
                .forEach(batch -> updatePartitionStatisticsBatch(table, mode, batch.stream().collect(toImmutableMap(Entry::getKey, Entry::getValue))));
    }

    private void updatePartitionStatisticsBatch(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        // Missing partitions are ignored
        Map<String, Partition> partitions = getPartitionsByNamesInternal(table, partitionUpdates.keySet()).entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().orElseThrow()));
        Map<String, Map<String, HiveColumnStatistics>> currentColumnStats = columnStatisticsProvider.getPartitionColumnStatistics(
                table.getDatabaseName(),
                table.getTableName(),
                partitionUpdates.keySet(),
                table.getDataColumns().stream().map(Column::getName).collect(toImmutableSet()));

        ImmutableList.Builder<BatchUpdatePartitionRequestEntry> partitionUpdateRequests = ImmutableList.builder();
        ImmutableSet.Builder<GlueColumnStatisticsProvider.PartitionStatisticsUpdate> columnStatisticsUpdates = ImmutableSet.builder();
        partitions.forEach((partitionName, partition) -> {
            PartitionStatistics update = partitionUpdates.get(partitionName);

            PartitionStatistics currentStatistics = new PartitionStatistics(getHiveBasicStatistics(partition.getParameters()), currentColumnStats.get(partitionName));
            PartitionStatistics updatedStatistics = mode.updatePartitionStatistics(currentStatistics, update);

            Map<String, String> updatedStatisticsParameters = updateStatisticsParameters(partition.getParameters(), updatedStatistics.basicStatistics());

            partition = Partition.builder(partition).setParameters(updatedStatisticsParameters).build();
            Map<String, HiveColumnStatistics> updatedColumnStatistics = updatedStatistics.columnStatistics();

            PartitionInput partitionInput = convertPartition(partition)
                    .toBuilder()
                    .parameters(partition.getParameters())
                    .build();

            partitionUpdateRequests.add(BatchUpdatePartitionRequestEntry.builder()
                    .partitionValueList(partition.getValues())
                    .partitionInput(partitionInput)
                    .build());
            columnStatisticsUpdates.add(new GlueColumnStatisticsProvider.PartitionStatisticsUpdate(partition, updatedColumnStatistics));
        });

        List<List<BatchUpdatePartitionRequestEntry>> partitionUpdateRequestsPartitioned = Lists.partition(partitionUpdateRequests.build(), BATCH_UPDATE_PARTITION_MAX_PAGE_SIZE);
        List<Future<BatchUpdatePartitionResponse>> partitionUpdateRequestsFutures = new ArrayList<>();
        partitionUpdateRequestsPartitioned.forEach(partitionUpdateRequestsPartition -> {
            // Update basic statistics
            partitionUpdateRequestsFutures.add(asyncGlueClient.batchUpdatePartition(BatchUpdatePartitionRequest.builder()
                            .databaseName(table.getDatabaseName())
                            .tableName(table.getTableName())
                            .entries(partitionUpdateRequestsPartition)
                            .build()));
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
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        try {
            glueClient.getUserDefinedFunction(GetUserDefinedFunctionRequest.builder()
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, signatureToken))
                    .build());
            return true;
        }
        catch (EntityNotFoundException e) {
            return false;
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return getFunctionsByPattern(databaseName, "trino__.*");
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return getFunctionsByPattern(databaseName, "trino__" + Pattern.quote(functionName) + "__.*");
    }

    private Collection<LanguageFunction> getFunctionsByPattern(String databaseName, String functionNamePattern)
    {
        try {
            return getPaginatedResults(
                    builder -> glueClient.getUserDefinedFunctions(builder.build()),
                    GetUserDefinedFunctionsRequest.builder()
                            .databaseName(databaseName)
                            .pattern(functionNamePattern)
                            .maxResults(AWS_GLUE_GET_FUNCTIONS_MAX_RESULTS),
                    GetUserDefinedFunctionsRequest.Builder::nextToken,
                    GetUserDefinedFunctionsResponse::nextToken)
                    .map(GetUserDefinedFunctionsResponse::userDefinedFunctions)
                    .flatMap(List::stream)
                    .map(GlueToTrinoConverter::convertFunction)
                    .collect(toImmutableList());
        }
        catch (EntityNotFoundException | AccessDeniedException e) {
            return ImmutableList.of();
        }
        catch (AwsServiceException e) {
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
            UserDefinedFunctionInput functionInput = convertFunction(functionName, function);
            glueClient.createUserDefinedFunction(CreateUserDefinedFunctionRequest.builder()
                        .databaseName(databaseName)
                        .functionInput(functionInput)
                        .build());
        }
        catch (AlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Function already exists", e);
        }
        catch (EntityNotFoundException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        try {
            UserDefinedFunctionInput functionInput = convertFunction(functionName, function);
            glueClient.updateUserDefinedFunction(UpdateUserDefinedFunctionRequest.builder()
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, function.signatureToken()))
                    .functionInput(functionInput)
                    .build());
        }
        catch (AwsServiceException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        try {
            glueClient.deleteUserDefinedFunction(DeleteUserDefinedFunctionRequest.builder()
                    .databaseName(databaseName)
                    .functionName(metastoreFunctionName(functionName, signatureToken))
                    .build());
        }
        catch (EntityNotFoundException e) {
            throw new TrinoException(FUNCTION_NOT_FOUND, "Function not found", e);
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

    private Stream<software.amazon.awssdk.services.glue.model.Table> getGlueTables(String databaseName)
    {
        return getPaginatedResults(
                builder -> glueClient.getTables(builder.build()),
                GetTablesRequest.builder()
                        .databaseName(databaseName)
                        .maxResults(AWS_GLUE_GET_TABLES_MAX_RESULTS),
                GetTablesRequest.Builder::nextToken,
                GetTablesResponse::nextToken)
                .map(GetTablesResponse::tableList)
                .flatMap(List::stream);
    }
}
