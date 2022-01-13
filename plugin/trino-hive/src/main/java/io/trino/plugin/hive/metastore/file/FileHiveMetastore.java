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
package io.trino.plugin.hive.metastore.file;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import io.airlift.json.JsonCodec;
import io.trino.plugin.hive.HdfsConfig;
import io.trino.plugin.hive.HdfsConfiguration;
import io.trino.plugin.hive.HdfsConfigurationInitializer;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HdfsEnvironment.HdfsContext;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveHdfsConfiguration;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.authentication.HiveIdentity;
import io.trino.plugin.hive.authentication.NoHdfsAuthentication;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.Database;
import io.trino.plugin.hive.metastore.HiveColumnStatistics;
import io.trino.plugin.hive.metastore.HiveMetastore;
import io.trino.plugin.hive.metastore.HivePrincipal;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo;
import io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import io.trino.plugin.hive.metastore.MetastoreConfig;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.PartitionWithStatistics;
import io.trino.plugin.hive.metastore.PrincipalPrivileges;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VersionCompatibility;
import io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static io.trino.plugin.hive.metastore.MetastoreUtil.makePartitionName;
import static io.trino.plugin.hive.metastore.MetastoreUtil.verifyCanDropColumn;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.SchemaType.DATABASE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.SchemaType.PARTITION;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastore.SchemaType.TABLE;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VERSION_COMPATIBILITY_CONFIG;
import static io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig.VersionCompatibility.UNSAFE_ASSUME_COMPATIBILITY;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.getHiveBasicStatistics;
import static io.trino.plugin.hive.metastore.thrift.ThriftMetastoreUtil.updateStatisticsParameters;
import static io.trino.plugin.hive.util.HiveUtil.DELTA_LAKE_PROVIDER;
import static io.trino.plugin.hive.util.HiveUtil.SPARK_TABLE_PROVIDER_KEY;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.common.FileUtils.unescapePathName;
import static org.apache.hadoop.hive.metastore.TableType.EXTERNAL_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MANAGED_TABLE;
import static org.apache.hadoop.hive.metastore.TableType.MATERIALIZED_VIEW;
import static org.apache.hadoop.hive.metastore.TableType.VIRTUAL_VIEW;

@ThreadSafe
public class FileHiveMetastore
        implements HiveMetastore
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String ADMIN_ROLE_NAME = "admin";
    private static final String TRINO_SCHEMA_FILE_NAME_SUFFIX = ".trinoSchema";
    private static final String TRINO_PERMISSIONS_DIRECTORY_NAME = ".trinoPermissions";
    public static final String ROLES_FILE_NAME = ".roles";
    public static final String ROLE_GRANTS_FILE_NAME = ".roleGrants";
    // todo there should be a way to manage the admins list
    private static final Set<String> ADMIN_USERS = ImmutableSet.of("admin", "hive", "hdfs");

    private final String currentVersion;
    private final VersionCompatibility versionCompatibility;
    private final HdfsEnvironment hdfsEnvironment;
    private final Path catalogDirectory;
    private final HdfsContext hdfsContext;
    private final boolean hideDeltaLakeTables;
    private final FileSystem metadataFileSystem;

    private final JsonCodec<DatabaseMetadata> databaseCodec = JsonCodec.jsonCodec(DatabaseMetadata.class);
    private final JsonCodec<TableMetadata> tableCodec = JsonCodec.jsonCodec(TableMetadata.class);
    private final JsonCodec<PartitionMetadata> partitionCodec = JsonCodec.jsonCodec(PartitionMetadata.class);
    private final JsonCodec<List<PermissionMetadata>> permissionsCodec = JsonCodec.listJsonCodec(PermissionMetadata.class);
    private final JsonCodec<List<String>> rolesCodec = JsonCodec.listJsonCodec(String.class);
    private final JsonCodec<List<RoleGrant>> roleGrantsCodec = JsonCodec.listJsonCodec(RoleGrant.class);

    @VisibleForTesting
    public static FileHiveMetastore createTestingFileHiveMetastore(File catalogDirectory)
    {
        HdfsConfig hdfsConfig = new HdfsConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hdfsConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, hdfsConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(
                new NodeVersion("testversion"),
                hdfsEnvironment,
                new MetastoreConfig(),
                new FileHiveMetastoreConfig()
                        .setCatalogDirectory(catalogDirectory.toURI().toString())
                        .setMetastoreUser("test"));
    }

    @Inject
    public FileHiveMetastore(NodeVersion nodeVersion, HdfsEnvironment hdfsEnvironment, MetastoreConfig metastoreConfig, FileHiveMetastoreConfig config)
    {
        this.currentVersion = requireNonNull(nodeVersion, "nodeVersion is null").toString();
        requireNonNull(config, "config is null");
        this.versionCompatibility = requireNonNull(config.getVersionCompatibility(), "config.getVersionCompatibility() is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.catalogDirectory = new Path(requireNonNull(config.getCatalogDirectory(), "catalogDirectory is null"));
        this.hdfsContext = new HdfsContext(ConnectorIdentity.ofUser(config.getMetastoreUser()));
        this.hideDeltaLakeTables = requireNonNull(metastoreConfig, "metastoreConfig is null").isHideDeltaLakeTables();
        try {
            metadataFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, this.catalogDirectory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void createDatabase(HiveIdentity identity, Database database)
    {
        requireNonNull(database, "database is null");

        if (database.getLocation().isPresent()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Database cannot be created with a location set");
        }

        verifyDatabaseNotExists(database.getDatabaseName());

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
        writeSchemaFile(DATABASE, databaseMetadataDirectory, databaseCodec, new DatabaseMetadata(currentVersion, database), false);
        try {
            metadataFileSystem.mkdirs(databaseMetadataDirectory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write database", e);
        }
    }

    @Override
    public synchronized void dropDatabase(HiveIdentity identity, String databaseName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");

        getRequiredDatabase(databaseName);
        if (!getAllTables(databaseName).isEmpty()) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Database " + databaseName + " is not empty");
        }

        // Either delete the entire database directory or just its metadata files
        if (deleteData) {
            deleteDirectoryAndSchema(DATABASE, getDatabaseMetadataDirectory(databaseName));
        }
        else {
            deleteSchemaFile(DATABASE, getDatabaseMetadataDirectory(databaseName));
        }
    }

    @Override
    public synchronized void renameDatabase(HiveIdentity identity, String databaseName, String newDatabaseName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");

        getRequiredDatabase(databaseName);
        verifyDatabaseNotExists(newDatabaseName);

        try {
            if (!metadataFileSystem.rename(getDatabaseMetadataDirectory(databaseName), getDatabaseMetadataDirectory(newDatabaseName))) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not rename database metadata directory");
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void setDatabaseOwner(HiveIdentity identity, String databaseName, HivePrincipal principal)
    {
        Database database = getRequiredDatabase(databaseName);
        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
        Database newDatabase = Database.builder(database)
                .setOwnerName(Optional.of(principal.getName()))
                .setOwnerType(Optional.of(principal.getType()))
                .build();

        writeSchemaFile(DATABASE, databaseMetadataDirectory, databaseCodec, new DatabaseMetadata(currentVersion, newDatabase), true);
    }

    @Override
    public synchronized Optional<Database> getDatabase(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        return readSchemaFile(DATABASE, databaseMetadataDirectory, databaseCodec)
                .map(databaseMetadata -> {
                    checkVersion(databaseMetadata.getWriterVersion());
                    return databaseMetadata.toDatabase(databaseName, databaseMetadataDirectory.toString());
                });
    }

    private Database getRequiredDatabase(String databaseName)
    {
        return getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
    }

    private void verifyDatabaseNotExists(String databaseName)
    {
        if (getDatabase(databaseName).isPresent()) {
            throw new SchemaAlreadyExistsException(databaseName);
        }
    }

    @Override
    public synchronized List<String> getAllDatabases()
    {
        List<String> databases = getChildSchemaDirectories(DATABASE, catalogDirectory).stream()
                .map(Path::getName)
                .collect(toList());
        return ImmutableList.copyOf(databases);
    }

    @Override
    public synchronized void createTable(HiveIdentity identity, Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyTableNotExists(table.getDatabaseName(), table.getTableName());

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        // validate table location
        if (table.getTableType().equals(VIRTUAL_VIEW.name())) {
            checkArgument(table.getStorage().getLocation().isEmpty(), "Storage location for view must be empty");
        }
        else if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!tableMetadataDirectory.equals(new Path(table.getStorage().getLocation()))) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Table directory must be " + tableMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            try {
                Path externalLocation = new Path(table.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, externalLocation);
                if (!externalFileSystem.isDirectory(externalLocation)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "External table location does not exist");
                }
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not validate external location", e);
            }
        }
        else if (!table.getTableType().equals(MATERIALIZED_VIEW.name())) {
            throw new TrinoException(NOT_SUPPORTED, "Table type not supported: " + table.getTableType());
        }

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, new TableMetadata(currentVersion, table), false);

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(new HivePrincipal(USER, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(new HivePrincipal(ROLE, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized Optional<Table> getTable(HiveIdentity identity, String databaseName, String tableName)
    {
        return getTable(databaseName, tableName);
    }

    private Optional<Table> getTable(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        return readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .map(tableMetadata -> {
                    checkVersion(tableMetadata.getWriterVersion());
                    return tableMetadata.toTable(databaseName, tableName, tableMetadataDirectory.toString());
                });
    }

    @Override
    public synchronized void setTableOwner(HiveIdentity identity, String databaseName, String tableName, HivePrincipal principal)
    {
        // TODO Add role support https://github.com/trinodb/trino/issues/5706
        if (principal.getType() != USER) {
            throw new TrinoException(NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }

        Table table = getRequiredTable(databaseName, tableName);
        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        Table newTable = Table.builder(table)
                .setOwner(Optional.of(principal.getName()))
                .build();

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, new TableMetadata(currentVersion, newTable), true);
    }

    @Override
    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(HiveIdentity identity, Table table)
    {
        return getTableStatistics(table.getDatabaseName(), table.getTableName());
    }

    private synchronized PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        checkVersion(tableMetadata.getWriterVersion());
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(tableMetadata.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = tableMetadata.getColumnStatistics();
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(HiveIdentity identity, Table table, List<Partition> partitions)
    {
        return partitions.stream()
                .collect(toImmutableMap(partition -> makePartitionName(table, partition), partition -> getPartitionStatistics(table, partition.getValues())));
    }

    private synchronized PartitionStatistics getPartitionStatistics(Table table, List<String> partitionValues)
    {
        Path partitionDirectory = getPartitionMetadataDirectory(table, ImmutableList.copyOf(partitionValues));
        PartitionMetadata partitionMetadata = readSchemaFile(PARTITION, partitionDirectory, partitionCodec)
                .orElseThrow(() -> new PartitionNotFoundException(table.getSchemaTableName(), partitionValues));
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(partitionMetadata.getParameters());
        return new PartitionStatistics(basicStatistics, partitionMetadata.getColumnStatistics());
    }

    private Table getRequiredTable(String databaseName, String tableName)
    {
        return getTable(databaseName, tableName)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
    }

    private void verifyTableNotExists(String newDatabaseName, String newTableName)
    {
        if (getTable(newDatabaseName, newTableName).isPresent()) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
    }

    @Override
    public synchronized void updateTableStatistics(HiveIdentity identity, String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics originalStatistics = getTableStatistics(databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(originalStatistics);

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        checkVersion(tableMetadata.getWriterVersion());

        TableMetadata updatedMetadata = tableMetadata
                .withParameters(currentVersion, updateStatisticsParameters(tableMetadata.getParameters(), updatedStatistics.getBasicStatistics()))
                .withColumnStatistics(currentVersion, updatedStatistics.getColumnStatistics());

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, updatedMetadata, true);
    }

    @Override
    public synchronized void updatePartitionStatistics(HiveIdentity identity, Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        updates.forEach((partitionName, update) -> {
            PartitionStatistics originalStatistics = getPartitionStatistics(table, extractPartitionValues(partitionName));
            PartitionStatistics updatedStatistics = update.apply(originalStatistics);

            List<String> partitionValues = extractPartitionValues(partitionName);
            Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
            PartitionMetadata partitionMetadata = readSchemaFile(PARTITION, partitionDirectory, partitionCodec)
                    .orElseThrow(() -> new PartitionNotFoundException(new SchemaTableName(table.getDatabaseName(), table.getTableName()), partitionValues));

            PartitionMetadata updatedMetadata = partitionMetadata
                    .withParameters(updateStatisticsParameters(partitionMetadata.getParameters(), updatedStatistics.getBasicStatistics()))
                    .withColumnStatistics(updatedStatistics.getColumnStatistics());

            writeSchemaFile(PARTITION, partitionDirectory, partitionCodec, updatedMetadata, true);
        });
    }

    @Override
    public synchronized List<String> getAllTables(String databaseName)
    {
        return listAllTables(databaseName).stream()
                .filter(hideDeltaLakeTables
                        ? Predicate.not(ImmutableSet.copyOf(getTablesWithParameter(databaseName, SPARK_TABLE_PROVIDER_KEY, DELTA_LAKE_PROVIDER))::contains)
                        : tableName -> true)
                .collect(toImmutableList());
    }

    @Override
    public synchronized List<String> getTablesWithParameter(String databaseName, String parameterKey, String parameterValue)
    {
        requireNonNull(parameterKey, "parameterKey is null");
        requireNonNull(parameterValue, "parameterValue is null");

        List<String> tables = listAllTables(databaseName);

        return tables.stream()
                .map(tableName -> getTable(databaseName, tableName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(table -> parameterValue.equals(table.getParameters().get(parameterKey)))
                .map(Table::getTableName)
                .collect(toImmutableList());
    }

    @GuardedBy("this")
    private List<String> listAllTables(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Optional<Database> database = getDatabase(databaseName);
        if (database.isEmpty()) {
            return ImmutableList.of();
        }

        Path databaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        List<String> tables = getChildSchemaDirectories(TABLE, databaseMetadataDirectory).stream()
                .map(Path::getName)
                .collect(toImmutableList());
        return tables;
    }

    @Override
    public synchronized List<String> getAllViews(String databaseName)
    {
        return getAllTables(databaseName).stream()
                .map(tableName -> getTable(databaseName, tableName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(table -> TableType.valueOf(table.getTableType()).equals(VIRTUAL_VIEW))
                .map(Table::getTableName)
                .collect(toImmutableList());
    }

    @Override
    public synchronized void dropTable(HiveIdentity identity, String databaseName, String tableName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Table table = getRequiredTable(databaseName, tableName);

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        // It is safe to delete the whole meta directory for external tables and views
        if (!table.getTableType().equals(MANAGED_TABLE.name()) || deleteData) {
            deleteDirectoryAndSchema(TABLE, tableMetadataDirectory);
        }
        else {
            // in this case we only want to delete the metadata of a managed table
            deleteSchemaFile(TABLE, tableMetadataDirectory);
            deleteTablePrivileges(table);
        }
    }

    @Override
    public synchronized void replaceTable(HiveIdentity identity, String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        Table table = getRequiredTable(databaseName, tableName);
        if (!table.getDatabaseName().equals(databaseName) || !table.getTableName().equals(tableName)) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Replacement table must have same name");
        }

        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, new TableMetadata(currentVersion, newTable), true);

        // replace existing permissions
        deleteTablePrivileges(table);

        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getUserPrivileges().asMap().entrySet()) {
            setTablePrivileges(new HivePrincipal(USER, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
        for (Entry<String, Collection<HivePrivilegeInfo>> entry : principalPrivileges.getRolePrivileges().asMap().entrySet()) {
            setTablePrivileges(new HivePrincipal(ROLE, entry.getKey()), table.getDatabaseName(), table.getTableName(), entry.getValue());
        }
    }

    @Override
    public synchronized void renameTable(HiveIdentity identity, String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");
        requireNonNull(newTableName, "newTableName is null");

        Table table = getRequiredTable(databaseName, tableName);
        getRequiredDatabase(newDatabaseName);

        // verify new table does not exist
        verifyTableNotExists(newDatabaseName, newTableName);

        Path oldPath = getTableMetadataDirectory(databaseName, tableName);
        Path newPath = getTableMetadataDirectory(newDatabaseName, newTableName);

        try {
            if (isIcebergTable(table)) {
                if (!metadataFileSystem.mkdirs(newPath)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Could not create new table directory");
                }
                // Iceberg metadata references files in old path, so these cannot be moved. Moving table description (metadata from metastore perspective) only.
                if (!metadataFileSystem.rename(getSchemaPath(TABLE, oldPath), getSchemaPath(TABLE, newPath))) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Could not rename table schema file");
                }
                // TODO drop data files when table is being dropped
            }
            else {
                if (!metadataFileSystem.rename(oldPath, newPath)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Could not rename table directory");
                }
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void commentTable(HiveIdentity identity, String databaseName, String tableName, Optional<String> comment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            Map<String, String> parameters = oldTable.getParameters().entrySet().stream()
                    .filter(entry -> !entry.getKey().equals(TABLE_COMMENT))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            comment.ifPresent(value -> parameters.put(TABLE_COMMENT, value));

            return oldTable.withParameters(currentVersion, parameters);
        });
    }

    @Override
    public synchronized void commentColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(columnName).isEmpty()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, columnName);
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (fieldSchema.getName().equals(columnName)) {
                    newDataColumns.add(new Column(columnName, fieldSchema.getType(), comment));
                }
                else {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(currentVersion, newDataColumns.build());
        });
    }

    @Override
    public synchronized void addColumn(HiveIdentity identity, String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(columnName).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "Column already exists: " + columnName);
            }

            return oldTable.withDataColumns(
                    currentVersion,
                    ImmutableList.<Column>builder()
                            .addAll(oldTable.getDataColumns())
                            .add(new Column(columnName, columnType, Optional.ofNullable(columnComment)))
                            .build());
        });
    }

    @Override
    public synchronized void renameColumn(HiveIdentity identity, String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(newColumnName).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "Column already exists: " + newColumnName);
            }
            if (oldTable.getColumn(oldColumnName).isEmpty()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, oldColumnName);
            }
            for (Column column : oldTable.getPartitionColumns()) {
                if (column.getName().equals(oldColumnName)) {
                    throw new TrinoException(NOT_SUPPORTED, "Renaming partition columns is not supported");
                }
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (fieldSchema.getName().equals(oldColumnName)) {
                    newDataColumns.add(new Column(newColumnName, fieldSchema.getType(), fieldSchema.getComment()));
                }
                else {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(currentVersion, newDataColumns.build());
        });
    }

    @Override
    public synchronized void dropColumn(HiveIdentity identity, String databaseName, String tableName, String columnName)
    {
        alterTable(databaseName, tableName, oldTable -> {
            verifyCanDropColumn(this, identity, databaseName, tableName, columnName);
            if (oldTable.getColumn(columnName).isEmpty()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, columnName);
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (!fieldSchema.getName().equals(columnName)) {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(currentVersion, newDataColumns.build());
        });
    }

    private void alterTable(String databaseName, String tableName, Function<TableMetadata, TableMetadata> alterFunction)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Path tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        TableMetadata oldTableSchema = readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        checkVersion(oldTableSchema.getWriterVersion());

        TableMetadata newTableSchema = alterFunction.apply(oldTableSchema);
        if (oldTableSchema == newTableSchema) {
            return;
        }

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, newTableSchema, true);
    }

    @Override
    public synchronized void addPartitions(HiveIdentity identity, String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");

        Table table = getRequiredTable(databaseName, tableName);

        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE).contains(tableType), "Invalid table type: %s", tableType);

        try {
            Map<Path, byte[]> schemaFiles = new LinkedHashMap<>();
            for (PartitionWithStatistics partitionWithStatistics : partitions) {
                Partition partition = partitionWithStatistics.getPartition();
                verifiedPartition(table, partition);
                Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
                Path schemaPath = getSchemaPath(PARTITION, partitionMetadataDirectory);
                if (metadataFileSystem.exists(schemaPath)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Partition already exists");
                }
                byte[] schemaJson = partitionCodec.toJsonBytes(new PartitionMetadata(table, partitionWithStatistics));
                schemaFiles.put(schemaPath, schemaJson);
            }

            Set<Path> createdFiles = new LinkedHashSet<>();
            try {
                for (Entry<Path, byte[]> entry : schemaFiles.entrySet()) {
                    try (OutputStream outputStream = metadataFileSystem.create(entry.getKey())) {
                        createdFiles.add(entry.getKey());
                        outputStream.write(entry.getValue());
                    }
                    catch (IOException e) {
                        throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write partition schema", e);
                    }
                }
            }
            catch (Throwable e) {
                for (Path createdFile : createdFiles) {
                    try {
                        metadataFileSystem.delete(createdFile, false);
                    }
                    catch (IOException ignored) {
                    }
                }
                throw e;
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void verifiedPartition(Table table, Partition partition)
    {
        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());

        if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!partitionMetadataDirectory.equals(new Path(partition.getStorage().getLocation()))) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Partition directory must be " + partitionMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            try {
                Path externalLocation = new Path(partition.getStorage().getLocation());
                FileSystem externalFileSystem = hdfsEnvironment.getFileSystem(hdfsContext, externalLocation);
                if (!externalFileSystem.isDirectory(externalLocation)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "External partition location does not exist");
                }
                if (isChildDirectory(catalogDirectory, externalLocation)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "External partition location cannot be inside the system metadata directory");
                }
            }
            catch (IOException e) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not validate external partition location", e);
            }
        }
        else {
            throw new TrinoException(NOT_SUPPORTED, "Partitions cannot be added to " + table.getTableType());
        }
    }

    @Override
    public synchronized void dropPartition(HiveIdentity identity, String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(identity, databaseName, tableName);
        if (tableReference.isEmpty()) {
            return;
        }
        Table table = tableReference.get();

        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partitionValues);
        if (deleteData) {
            deleteDirectoryAndSchema(PARTITION, partitionMetadataDirectory);
        }
        else {
            deleteSchemaFile(PARTITION, partitionMetadataDirectory);
        }
    }

    @Override
    public synchronized void alterPartition(HiveIdentity identity, String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        Table table = getRequiredTable(databaseName, tableName);

        Partition partition = partitionWithStatistics.getPartition();
        verifiedPartition(table, partition);

        Path partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
        writeSchemaFile(PARTITION, partitionMetadataDirectory, partitionCodec, new PartitionMetadata(table, partitionWithStatistics), true);
    }

    @Override
    public synchronized void createRole(String role, String grantor)
    {
        Set<String> roles = new HashSet<>(listRoles());
        roles.add(role);
        writeFile("roles", getRolesFile(), rolesCodec, ImmutableList.copyOf(roles), true);
    }

    @Override
    public synchronized void dropRole(String role)
    {
        Set<String> roles = new HashSet<>(listRoles());
        roles.remove(role);
        writeFile("roles", getRolesFile(), rolesCodec, ImmutableList.copyOf(roles), true);
        Set<RoleGrant> grants = listRoleGrantsSanitized();
        writeRoleGrantsFile(grants);
    }

    @Override
    public synchronized Set<String> listRoles()
    {
        Set<String> roles = new HashSet<>();
        // Hive SQL standard assumes admin role already exists, so until that is fixed always add it here
        roles.add("admin");
        readFile("roles", getRolesFile(), rolesCodec).ifPresent(roles::addAll);
        return ImmutableSet.copyOf(roles);
    }

    @Override
    public synchronized void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        Set<String> existingRoles = listRoles();
        Set<RoleGrant> existingGrants = listRoleGrantsSanitized();
        Set<RoleGrant> modifiedGrants = new HashSet<>(existingGrants);
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                checkArgument(existingRoles.contains(role), "Role does not exist: %s", role);
                if (grantee.getType() == ROLE) {
                    checkArgument(existingRoles.contains(grantee.getName()), "Role does not exist: %s", grantee.getName());
                }

                RoleGrant grantWithAdminOption = new RoleGrant(grantee.toTrinoPrincipal(), role, true);
                RoleGrant grantWithoutAdminOption = new RoleGrant(grantee.toTrinoPrincipal(), role, false);

                if (adminOption) {
                    modifiedGrants.remove(grantWithoutAdminOption);
                    modifiedGrants.add(grantWithAdminOption);
                }
                else {
                    modifiedGrants.remove(grantWithAdminOption);
                    modifiedGrants.add(grantWithoutAdminOption);
                }
            }
        }
        modifiedGrants = removeDuplicatedEntries(modifiedGrants);
        if (!existingGrants.equals(modifiedGrants)) {
            writeRoleGrantsFile(modifiedGrants);
        }
    }

    @Override
    public synchronized void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        Set<RoleGrant> existingGrants = listRoleGrantsSanitized();
        Set<RoleGrant> modifiedGrants = new HashSet<>(existingGrants);
        for (HivePrincipal grantee : grantees) {
            for (String role : roles) {
                RoleGrant grantWithAdminOption = new RoleGrant(grantee.toTrinoPrincipal(), role, true);
                RoleGrant grantWithoutAdminOption = new RoleGrant(grantee.toTrinoPrincipal(), role, false);

                if (modifiedGrants.contains(grantWithAdminOption) || modifiedGrants.contains(grantWithoutAdminOption)) {
                    if (adminOption) {
                        modifiedGrants.remove(grantWithAdminOption);
                        modifiedGrants.add(grantWithoutAdminOption);
                    }
                    else {
                        modifiedGrants.remove(grantWithAdminOption);
                        modifiedGrants.remove(grantWithoutAdminOption);
                    }
                }
            }
        }
        modifiedGrants = removeDuplicatedEntries(modifiedGrants);
        if (!existingGrants.equals(modifiedGrants)) {
            writeRoleGrantsFile(modifiedGrants);
        }
    }

    @Override
    public synchronized Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return listRoleGrantsSanitized().stream()
                .filter(grant -> grant.getRoleName().equals(role))
                .collect(toImmutableSet());
    }

    @Override
    public synchronized Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        ImmutableSet.Builder<RoleGrant> result = ImmutableSet.builder();
        if (principal.getType() == USER) {
            result.add(new RoleGrant(principal.toTrinoPrincipal(), PUBLIC_ROLE_NAME, false));
            if (ADMIN_USERS.contains(principal.getName())) {
                result.add(new RoleGrant(principal.toTrinoPrincipal(), ADMIN_ROLE_NAME, true));
            }
        }
        result.addAll(listRoleGrantsSanitized().stream()
                .filter(grant -> HivePrincipal.from(grant.getGrantee()).equals(principal))
                .collect(toSet()));
        return result.build();
    }

    private synchronized Set<RoleGrant> listRoleGrantsSanitized()
    {
        Set<RoleGrant> grants = readRoleGrantsFile();
        Set<String> existingRoles = listRoles();
        return removeDuplicatedEntries(removeNonExistingRoles(grants, existingRoles));
    }

    private Set<RoleGrant> removeDuplicatedEntries(Set<RoleGrant> grants)
    {
        Map<RoleGranteeTuple, RoleGrant> map = new HashMap<>();
        for (RoleGrant grant : grants) {
            RoleGranteeTuple tuple = new RoleGranteeTuple(grant.getRoleName(), HivePrincipal.from(grant.getGrantee()));
            map.merge(tuple, grant, (first, second) -> first.isGrantable() ? first : second);
        }
        return ImmutableSet.copyOf(map.values());
    }

    private static Set<RoleGrant> removeNonExistingRoles(Set<RoleGrant> grants, Set<String> existingRoles)
    {
        ImmutableSet.Builder<RoleGrant> result = ImmutableSet.builder();
        for (RoleGrant grant : grants) {
            if (!existingRoles.contains(grant.getRoleName())) {
                continue;
            }
            HivePrincipal grantee = HivePrincipal.from(grant.getGrantee());
            if (grantee.getType() == ROLE && !existingRoles.contains(grantee.getName())) {
                continue;
            }
            result.add(grant);
        }
        return result.build();
    }

    private Set<RoleGrant> readRoleGrantsFile()
    {
        return ImmutableSet.copyOf(readFile("roleGrants", getRoleGrantsFile(), roleGrantsCodec).orElse(ImmutableList.of()));
    }

    private void writeRoleGrantsFile(Set<RoleGrant> roleGrants)
    {
        writeFile("roleGrants", getRoleGrantsFile(), roleGrantsCodec, ImmutableList.copyOf(roleGrants), true);
    }

    private synchronized Optional<List<String>> getAllPartitionNames(HiveIdentity identity, String databaseName, String tableName)
    {
        requireNonNull(identity, "identity is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Optional<Table> tableReference = getTable(identity, databaseName, tableName);
        if (tableReference.isEmpty()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Path tableMetadataDirectory = getTableMetadataDirectory(table);

        List<ArrayDeque<String>> partitions = listPartitions(tableMetadataDirectory, table.getPartitionColumns());

        List<String> partitionNames = partitions.stream()
                .map(partitionValues -> makePartitionName(table.getPartitionColumns(), ImmutableList.copyOf(partitionValues)))
                .filter(partitionName -> isValidPartition(table, partitionName))
                .collect(toList());

        return Optional.of(ImmutableList.copyOf(partitionNames));
    }

    private boolean isValidPartition(Table table, String partitionName)
    {
        try {
            return metadataFileSystem.exists(getSchemaPath(PARTITION, getPartitionMetadataDirectory(table, partitionName)));
        }
        catch (IOException e) {
            return false;
        }
    }

    private List<ArrayDeque<String>> listPartitions(Path director, List<Column> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of();
        }

        try {
            String directoryPrefix = partitionColumns.get(0).getName() + '=';

            List<ArrayDeque<String>> partitionValues = new ArrayList<>();
            for (FileStatus fileStatus : metadataFileSystem.listStatus(director)) {
                if (!fileStatus.isDirectory()) {
                    continue;
                }
                if (!fileStatus.getPath().getName().startsWith(directoryPrefix)) {
                    continue;
                }

                List<ArrayDeque<String>> childPartitionValues;
                if (partitionColumns.size() == 1) {
                    childPartitionValues = ImmutableList.of(new ArrayDeque<>());
                }
                else {
                    childPartitionValues = listPartitions(fileStatus.getPath(), partitionColumns.subList(1, partitionColumns.size()));
                }

                String value = unescapePathName(fileStatus.getPath().getName().substring(directoryPrefix.length()));
                for (ArrayDeque<String> childPartition : childPartitionValues) {
                    childPartition.addFirst(value);
                    partitionValues.add(childPartition);
                }
            }
            return partitionValues;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Error listing partition directories", e);
        }
    }

    @Override
    public synchronized Optional<Partition> getPartition(HiveIdentity identity, Table table, List<String> partitionValues)
    {
        requireNonNull(table, "table is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Path partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        return readSchemaFile(PARTITION, partitionDirectory, partitionCodec)
                .map(partitionMetadata -> partitionMetadata.toPartition(table.getDatabaseName(), table.getTableName(), partitionValues, partitionDirectory.toString()));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            HiveIdentity identity,
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return getAllPartitionNames(identity, databaseName, tableName);
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(HiveIdentity identity, Table table, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            builder.put(partitionName, getPartition(identity, table, partitionValues));
        }
        return builder.build();
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        Table table = getRequiredTable(databaseName, tableName);
        Path permissionsDirectory = getPermissionsDirectory(table);
        if (principal.isEmpty()) {
            Builder<HivePrivilegeInfo> privileges = ImmutableSet.<HivePrivilegeInfo>builder()
                    .addAll(readAllPermissions(permissionsDirectory));
            tableOwner.ifPresent(owner -> privileges.add(new HivePrivilegeInfo(OWNERSHIP, true, new HivePrincipal(USER, owner), new HivePrincipal(USER, owner))));
            return privileges.build();
        }
        ImmutableSet.Builder<HivePrivilegeInfo> result = ImmutableSet.builder();
        if (principal.get().getType() == USER && table.getOwner().orElseThrow().equals(principal.get().getName())) {
            result.add(new HivePrivilegeInfo(OWNERSHIP, true, principal.get(), principal.get()));
        }
        result.addAll(readPermissionsFile(getPermissionsPath(permissionsDirectory, principal.get())));
        return result.build();
    }

    @Override
    public synchronized void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        setTablePrivileges(
                grantee,
                databaseName,
                tableName,
                privileges.stream()
                        .map(privilege -> new HivePrivilegeInfo(privilege, grantOption, grantor, grantee))
                        .collect(toImmutableList()));
    }

    @Override
    public synchronized void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        Set<HivePrivilegeInfo> currentPrivileges = listTablePrivileges(databaseName, tableName, Optional.of(tableOwner), Optional.of(grantee));
        Set<HivePrivilegeInfo> privilegesToRemove = privileges.stream()
                .map(p -> new HivePrivilegeInfo(p, grantOption, grantor, grantee))
                .collect(toImmutableSet());

        setTablePrivileges(grantee, databaseName, tableName, Sets.difference(currentPrivileges, privilegesToRemove));
    }

    @Override
    public boolean isImpersonationEnabled()
    {
        return false;
    }

    private synchronized void setTablePrivileges(
            HivePrincipal grantee,
            String databaseName,
            String tableName,
            Collection<HivePrivilegeInfo> privileges)
    {
        requireNonNull(grantee, "grantee is null");
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(privileges, "privileges is null");

        try {
            Table table = getRequiredTable(databaseName, tableName);

            Path permissionsDirectory = getPermissionsDirectory(table);

            boolean created = metadataFileSystem.mkdirs(permissionsDirectory);
            if (!created && !metadataFileSystem.isDirectory(permissionsDirectory)) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not create permissions directory");
            }

            Path permissionFilePath = getPermissionsPath(permissionsDirectory, grantee);
            List<PermissionMetadata> permissions = privileges.stream()
                    .map(hivePrivilegeInfo -> new PermissionMetadata(hivePrivilegeInfo.getHivePrivilege(), hivePrivilegeInfo.isGrantOption(), grantee))
                    .collect(toList());
            writeFile("permissions", permissionFilePath, permissionsCodec, permissions, true);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private synchronized void deleteTablePrivileges(Table table)
    {
        try {
            Path permissionsDirectory = getPermissionsDirectory(table);
            metadataFileSystem.delete(permissionsDirectory, true);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete table permissions", e);
        }
    }

    private List<Path> getChildSchemaDirectories(SchemaType type, Path metadataDirectory)
    {
        try {
            if (!metadataFileSystem.isDirectory(metadataDirectory)) {
                return ImmutableList.of();
            }

            ImmutableList.Builder<Path> childSchemaDirectories = ImmutableList.builder();
            for (FileStatus child : metadataFileSystem.listStatus(metadataDirectory)) {
                if (!child.isDirectory()) {
                    continue;
                }
                Path childPath = child.getPath();
                if (childPath.getName().startsWith(".")) {
                    continue;
                }
                if (metadataFileSystem.isFile(getSchemaPath(type, childPath))) {
                    childSchemaDirectories.add(childPath);
                }
            }
            return childSchemaDirectories.build();
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private Set<HivePrivilegeInfo> readPermissionsFile(Path permissionFilePath)
    {
        return readFile("permissions", permissionFilePath, permissionsCodec).orElse(ImmutableList.of()).stream()
                .map(PermissionMetadata::toHivePrivilegeInfo)
                .collect(toImmutableSet());
    }

    private Set<HivePrivilegeInfo> readAllPermissions(Path permissionsDirectory)
    {
        try {
            return Arrays.stream(metadataFileSystem.listStatus(permissionsDirectory))
                    .filter(FileStatus::isFile)
                    .filter(file -> !file.getPath().getName().startsWith("."))
                    .flatMap(file -> readPermissionsFile(file.getPath()).stream())
                    .collect(toImmutableSet());
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void deleteDirectoryAndSchema(SchemaType type, Path metadataDirectory)
    {
        try {
            Path schemaPath = getSchemaPath(type, metadataDirectory);
            if (!metadataFileSystem.isFile(schemaPath)) {
                // if there is no schema file, assume this is not a database, partition or table
                return;
            }

            // Delete the schema file first, so it can never exist after the directory is deleted.
            // (For cases when the schema file isn't in the metadata directory.)
            deleteSchemaFile(type, metadataDirectory);

            if (!metadataFileSystem.delete(metadataDirectory, true)) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete metadata directory");
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void checkVersion(Optional<String> writerVersion)
    {
        if (writerVersion.isPresent() && writerVersion.get().equals(currentVersion)) {
            return;
        }
        if (versionCompatibility == UNSAFE_ASSUME_COMPATIBILITY) {
            return;
        }
        throw new RuntimeException(format(
                "The metadata file was written with %s while current version is %s. " +
                        "File metastore provides no compatibility for metadata written with a different version. " +
                        "You can disable this check by setting '%s=%s' configuration property.",
                writerVersion
                        .map(version -> "version " + version)
                        .orElse("unknown version"),
                currentVersion,
                VERSION_COMPATIBILITY_CONFIG,
                UNSAFE_ASSUME_COMPATIBILITY));
    }

    private <T> Optional<T> readSchemaFile(SchemaType type, Path metadataDirectory, JsonCodec<T> codec)
    {
        return readFile(type + " schema", getSchemaPath(type, metadataDirectory), codec);
    }

    private <T> Optional<T> readFile(String type, Path path, JsonCodec<T> codec)
    {
        try {
            if (!metadataFileSystem.isFile(path)) {
                return Optional.empty();
            }

            try (FSDataInputStream inputStream = metadataFileSystem.open(path)) {
                byte[] json = ByteStreams.toByteArray(inputStream);
                return Optional.of(codec.fromJson(json));
            }
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not read " + type, e);
        }
    }

    private <T> void writeSchemaFile(SchemaType type, Path directory, JsonCodec<T> codec, T value, boolean overwrite)
    {
        writeFile(type + " schema", getSchemaPath(type, directory), codec, value, overwrite);
    }

    private <T> void writeFile(String type, Path path, JsonCodec<T> codec, T value, boolean overwrite)
    {
        try {
            byte[] json = codec.toJsonBytes(value);

            if (!overwrite) {
                if (metadataFileSystem.exists(path)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, type + " file already exists");
                }
            }

            metadataFileSystem.mkdirs(path.getParent());

            // todo implement safer overwrite code
            try (OutputStream outputStream = metadataFileSystem.create(path, overwrite)) {
                outputStream.write(json);
            }
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write " + type, e);
        }
    }

    private void deleteSchemaFile(SchemaType type, Path metadataDirectory)
    {
        try {
            if (!metadataFileSystem.delete(getSchemaPath(type, metadataDirectory), false)) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete " + type + " schema");
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete " + type + " schema", e);
        }
    }

    private Path getDatabaseMetadataDirectory(String databaseName)
    {
        return new Path(catalogDirectory, databaseName);
    }

    private Path getTableMetadataDirectory(Table table)
    {
        return getTableMetadataDirectory(table.getDatabaseName(), table.getTableName());
    }

    private Path getTableMetadataDirectory(String databaseName, String tableName)
    {
        return new Path(getDatabaseMetadataDirectory(databaseName), tableName);
    }

    private Path getPartitionMetadataDirectory(Table table, List<String> values)
    {
        String partitionName = makePartitionName(table.getPartitionColumns(), values);
        return getPartitionMetadataDirectory(table, partitionName);
    }

    private Path getPartitionMetadataDirectory(Table table, String partitionName)
    {
        Path tableMetadataDirectory = getTableMetadataDirectory(table);
        return new Path(tableMetadataDirectory, partitionName);
    }

    private Path getPermissionsDirectory(Table table)
    {
        return new Path(getTableMetadataDirectory(table), TRINO_PERMISSIONS_DIRECTORY_NAME);
    }

    private static Path getPermissionsPath(Path permissionsDirectory, HivePrincipal grantee)
    {
        return new Path(permissionsDirectory, grantee.getType().toString().toLowerCase(Locale.US) + "_" + grantee.getName());
    }

    private Path getRolesFile()
    {
        return new Path(catalogDirectory, ROLES_FILE_NAME);
    }

    private Path getRoleGrantsFile()
    {
        return new Path(catalogDirectory, ROLE_GRANTS_FILE_NAME);
    }

    private static Path getSchemaPath(SchemaType type, Path metadataDirectory)
    {
        if (type == DATABASE) {
            return new Path(
                    requireNonNull(metadataDirectory.getParent(), "Can't use root directory as database path"),
                    format(".%s%s", metadataDirectory.getName(), TRINO_SCHEMA_FILE_NAME_SUFFIX));
        }
        return new Path(metadataDirectory, TRINO_SCHEMA_FILE_NAME_SUFFIX);
    }

    private static boolean isChildDirectory(Path parentDirectory, Path childDirectory)
    {
        if (parentDirectory.equals(childDirectory)) {
            return true;
        }
        if (childDirectory.isRoot()) {
            return false;
        }
        return isChildDirectory(parentDirectory, childDirectory.getParent());
    }

    private static class RoleGranteeTuple
    {
        private final String role;
        private final HivePrincipal grantee;

        private RoleGranteeTuple(String role, HivePrincipal grantee)
        {
            this.role = requireNonNull(role, "role is null");
            this.grantee = requireNonNull(grantee, "grantee is null");
        }

        public String getRole()
        {
            return role;
        }

        public HivePrincipal getGrantee()
        {
            return grantee;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            RoleGranteeTuple that = (RoleGranteeTuple) o;
            return Objects.equals(role, that.role) &&
                    Objects.equals(grantee, that.grantee);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(role, grantee);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("role", role)
                    .add("grantee", grantee)
                    .toString();
        }
    }

    // Visible to allow import into this file
    enum SchemaType
    {
        DATABASE, TABLE, PARTITION;

        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }
    }
}
