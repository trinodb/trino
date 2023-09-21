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

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.errorprone.annotations.ThreadSafe;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.airlift.json.JsonCodec;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.plugin.hive.HiveBasicStatistics;
import io.trino.plugin.hive.HiveColumnStatisticType;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.NodeVersion;
import io.trino.plugin.hive.PartitionNotFoundException;
import io.trino.plugin.hive.PartitionStatistics;
import io.trino.plugin.hive.SchemaAlreadyExistsException;
import io.trino.plugin.hive.TableAlreadyExistsException;
import io.trino.plugin.hive.TableType;
import io.trino.plugin.hive.ViewReaderUtil;
import io.trino.plugin.hive.acid.AcidTransaction;
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
import io.trino.spi.type.Type;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.plugin.hive.HiveMetadata.TABLE_COMMENT;
import static io.trino.plugin.hive.HivePartitionManager.extractPartitionValues;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hive.TableType.MANAGED_TABLE;
import static io.trino.plugin.hive.TableType.MATERIALIZED_VIEW;
import static io.trino.plugin.hive.ViewReaderUtil.isSomeKindOfAView;
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
import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.security.PrincipalType.ROLE;
import static io.trino.spi.security.PrincipalType.USER;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

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

    // 128 is equals to the max database name length of Thrift Hive metastore
    private static final int MAX_NAME_LENGTH = 128;

    private final String currentVersion;
    private final VersionCompatibility versionCompatibility;
    private final TrinoFileSystem fileSystem;
    private final Location catalogDirectory;
    private final boolean disableLocationChecks;
    private final boolean hideDeltaLakeTables;

    private final JsonCodec<DatabaseMetadata> databaseCodec = JsonCodec.jsonCodec(DatabaseMetadata.class);
    private final JsonCodec<TableMetadata> tableCodec = JsonCodec.jsonCodec(TableMetadata.class);
    private final JsonCodec<PartitionMetadata> partitionCodec = JsonCodec.jsonCodec(PartitionMetadata.class);
    private final JsonCodec<List<PermissionMetadata>> permissionsCodec = JsonCodec.listJsonCodec(PermissionMetadata.class);
    private final JsonCodec<List<String>> rolesCodec = JsonCodec.listJsonCodec(String.class);
    private final JsonCodec<List<RoleGrant>> roleGrantsCodec = JsonCodec.listJsonCodec(RoleGrant.class);

    // TODO Remove this speed-up workaround once that https://github.com/trinodb/trino/issues/13115 gets implemented
    private final LoadingCache<String, List<String>> listTablesCache;

    public FileHiveMetastore(NodeVersion nodeVersion, TrinoFileSystemFactory fileSystemFactory, boolean hideDeltaLakeTables, FileHiveMetastoreConfig config)
    {
        this.currentVersion = nodeVersion.toString();
        this.versionCompatibility = requireNonNull(config.getVersionCompatibility(), "config.getVersionCompatibility() is null");
        this.fileSystem = fileSystemFactory.create(ConnectorIdentity.ofUser(config.getMetastoreUser()));
        this.catalogDirectory = Location.of(requireNonNull(config.getCatalogDirectory(), "catalogDirectory is null"));
        this.disableLocationChecks = config.isDisableLocationChecks();
        this.hideDeltaLakeTables = hideDeltaLakeTables;

        listTablesCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(10, SECONDS)
                .build(CacheLoader.from(this::doListAllTables));
    }

    @Override
    public synchronized void createDatabase(Database database)
    {
        requireNonNull(database, "database is null");
        database = new Database(
                // Store name in lowercase for compatibility with HMS (and Glue)
                database.getDatabaseName().toLowerCase(ENGLISH),
                database.getLocation(),
                database.getOwnerName(),
                database.getOwnerType(),
                database.getComment(),
                database.getParameters());

        verifyDatabaseNameLength(database.getDatabaseName());
        verifyDatabaseNotExists(database.getDatabaseName());

        Location databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
        writeSchemaFile(DATABASE, databaseMetadataDirectory, databaseCodec, new DatabaseMetadata(currentVersion, database), false);
        try {
            fileSystem.createDirectory(databaseMetadataDirectory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write database", e);
        }
    }

    @Override
    public synchronized void dropDatabase(String databaseName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");

        // Database names are stored lowercase. Accept non-lowercase name for compatibility with HMS (and Glue)
        databaseName = databaseName.toLowerCase(ENGLISH);

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
    public synchronized void renameDatabase(String databaseName, String newDatabaseName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");

        verifyDatabaseNameLength(newDatabaseName);
        getRequiredDatabase(databaseName);
        verifyDatabaseNotExists(newDatabaseName);

        Location oldDatabaseMetadataDirectory = getDatabaseMetadataDirectory(databaseName);
        Location newDatabaseMetadataDirectory = getDatabaseMetadataDirectory(newDatabaseName);
        try {
            renameSchemaFile(DATABASE, oldDatabaseMetadataDirectory, newDatabaseMetadataDirectory);
            fileSystem.renameDirectory(oldDatabaseMetadataDirectory, newDatabaseMetadataDirectory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        Database database = getRequiredDatabase(databaseName);
        Location databaseMetadataDirectory = getDatabaseMetadataDirectory(database.getDatabaseName());
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

        // Database names are stored lowercase. Accept non-lowercase name for compatibility with HMS (and Glue)
        String normalizedName = databaseName.toLowerCase(ENGLISH);

        Location databaseMetadataDirectory = getDatabaseMetadataDirectory(normalizedName);
        return readSchemaFile(DATABASE, databaseMetadataDirectory, databaseCodec)
                .map(databaseMetadata -> {
                    checkVersion(databaseMetadata.getWriterVersion());
                    return databaseMetadata.toDatabase(normalizedName, databaseMetadataDirectory.toString());
                });
    }

    private Database getRequiredDatabase(String databaseName)
    {
        return getDatabase(databaseName)
                .orElseThrow(() -> new SchemaNotFoundException(databaseName));
    }

    private void verifyDatabaseNameLength(String databaseName)
    {
        if (databaseName.length() > MAX_NAME_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED, format("Schema name must be shorter than or equal to '%s' characters but got '%s'", MAX_NAME_LENGTH, databaseName.length()));
        }
    }

    private void verifyTableNameLength(String tableName)
    {
        if (tableName.length() > MAX_NAME_LENGTH) {
            throw new TrinoException(NOT_SUPPORTED, format("Table name must be shorter than or equal to '%s' characters but got '%s'", MAX_NAME_LENGTH, tableName.length()));
        }
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
        try {
            String prefix = catalogDirectory.toString();
            Set<String> databases = new HashSet<>();

            FileIterator iterator = fileSystem.listFiles(catalogDirectory);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();

                String child = location.toString().substring(prefix.length());
                if (child.startsWith("/")) {
                    child = child.substring(1);
                }

                int length = child.length() - TRINO_SCHEMA_FILE_NAME_SUFFIX.length();
                if ((length > 1) && !child.contains("/") && child.startsWith(".") &&
                        child.endsWith(TRINO_SCHEMA_FILE_NAME_SUFFIX)) {
                    databases.add(child.substring(1, length));
                }
            }

            return ImmutableList.copyOf(databases);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        verifyTableNameLength(table.getTableName());
        verifyDatabaseExists(table.getDatabaseName());
        verifyTableNotExists(table.getDatabaseName(), table.getTableName());

        Location tableMetadataDirectory = getTableMetadataDirectory(table);

        // validate table location
        if (isSomeKindOfAView(table)) {
            checkArgument(table.getStorage().getLocation().isEmpty(), "Storage location for view must be empty");
        }
        else if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!disableLocationChecks && !table.getStorage().getLocation().contains(tableMetadataDirectory.toString())) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Table directory must be " + tableMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            if (!disableLocationChecks) {
                try {
                    Location externalLocation = Location.of(table.getStorage().getLocation());
                    if (!fileSystem.directoryExists(externalLocation).orElse(true)) {
                        throw new TrinoException(HIVE_METASTORE_ERROR, "External table location does not exist");
                    }
                }
                catch (IOException e) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Could not validate external location", e);
                }
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
    public synchronized Optional<Table> getTable(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Location tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        return readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .map(tableMetadata -> {
                    checkVersion(tableMetadata.getWriterVersion());
                    return tableMetadata.toTable(databaseName, tableName, tableMetadataDirectory.toString());
                });
    }

    @Override
    public synchronized void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        // TODO Add role support https://github.com/trinodb/trino/issues/5706
        if (principal.getType() != USER) {
            throw new TrinoException(NOT_SUPPORTED, "Setting table owner type as a role is not supported");
        }

        Table table = getRequiredTable(databaseName, tableName);
        Location tableMetadataDirectory = getTableMetadataDirectory(table);
        Table newTable = Table.builder(table)
                .setOwner(Optional.of(principal.getName()))
                .build();

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, new TableMetadata(currentVersion, newTable), true);
    }

    @Override
    public Set<HiveColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ThriftMetastoreUtil.getSupportedColumnStatistics(type);
    }

    @Override
    public synchronized PartitionStatistics getTableStatistics(Table table)
    {
        return getTableStatistics(table.getDatabaseName(), table.getTableName());
    }

    private synchronized PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        Location tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        checkVersion(tableMetadata.getWriterVersion());
        HiveBasicStatistics basicStatistics = getHiveBasicStatistics(tableMetadata.getParameters());
        Map<String, HiveColumnStatistics> columnStatistics = tableMetadata.getColumnStatistics();
        return new PartitionStatistics(basicStatistics, columnStatistics);
    }

    @Override
    public synchronized Map<String, PartitionStatistics> getPartitionStatistics(Table table, List<Partition> partitions)
    {
        return partitions.stream()
                .collect(toImmutableMap(partition -> makePartitionName(table, partition), partition -> getPartitionStatisticsInternal(table, partition.getValues())));
    }

    private synchronized PartitionStatistics getPartitionStatisticsInternal(Table table, List<String> partitionValues)
    {
        Location partitionDirectory = getPartitionMetadataDirectory(table, ImmutableList.copyOf(partitionValues));
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

    private void verifyDatabaseExists(String databaseName)
    {
        if (getDatabase(databaseName).isEmpty()) {
            throw new SchemaNotFoundException(databaseName);
        }
    }

    private void verifyTableNotExists(String newDatabaseName, String newTableName)
    {
        if (getTable(newDatabaseName, newTableName).isPresent()) {
            throw new TableAlreadyExistsException(new SchemaTableName(newDatabaseName, newTableName));
        }
    }

    @Override
    public synchronized void updateTableStatistics(String databaseName, String tableName, AcidTransaction transaction, Function<PartitionStatistics, PartitionStatistics> update)
    {
        PartitionStatistics originalStatistics = getTableStatistics(databaseName, tableName);
        PartitionStatistics updatedStatistics = update.apply(originalStatistics);

        Location tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);
        TableMetadata tableMetadata = readSchemaFile(TABLE, tableMetadataDirectory, tableCodec)
                .orElseThrow(() -> new TableNotFoundException(new SchemaTableName(databaseName, tableName)));
        checkVersion(tableMetadata.getWriterVersion());

        TableMetadata updatedMetadata = tableMetadata
                .withParameters(currentVersion, updateStatisticsParameters(tableMetadata.getParameters(), updatedStatistics.getBasicStatistics()))
                .withColumnStatistics(currentVersion, updatedStatistics.getColumnStatistics());

        writeSchemaFile(TABLE, tableMetadataDirectory, tableCodec, updatedMetadata, true);
    }

    @Override
    public synchronized void updatePartitionStatistics(Table table, Map<String, Function<PartitionStatistics, PartitionStatistics>> updates)
    {
        updates.forEach((partitionName, update) -> {
            PartitionStatistics originalStatistics = getPartitionStatisticsInternal(table, extractPartitionValues(partitionName));
            PartitionStatistics updatedStatistics = update.apply(originalStatistics);

            List<String> partitionValues = extractPartitionValues(partitionName);
            Location partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
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
    public Optional<List<SchemaTableName>> getAllTables()
    {
        return Optional.empty();
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
        return listTablesCache.getUnchecked(databaseName);
    }

    @GuardedBy("this")
    private List<String> doListAllTables(String databaseName)
    {
        requireNonNull(databaseName, "databaseName is null");

        Optional<Database> database = getDatabase(databaseName);
        if (database.isEmpty()) {
            return ImmutableList.of();
        }

        Location metadataDirectory = getDatabaseMetadataDirectory(databaseName);
        try {
            String prefix = metadataDirectory.toString();
            Set<String> tables = new HashSet<>();

            FileIterator iterator = fileSystem.listFiles(metadataDirectory);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();

                String child = location.toString().substring(prefix.length());
                if (child.startsWith("/")) {
                    child = child.substring(1);
                }

                if (child.startsWith(".") || (child.indexOf('/') != child.lastIndexOf('/'))) {
                    continue;
                }

                int length = child.length() - TRINO_SCHEMA_FILE_NAME_SUFFIX.length() - 1;
                if ((length >= 1) && child.endsWith("/" + TRINO_SCHEMA_FILE_NAME_SUFFIX)) {
                    tables.add(child.substring(0, length));
                }
            }

            return ImmutableList.copyOf(tables);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    @Override
    public synchronized List<String> getAllViews(String databaseName)
    {
        return getAllTables(databaseName).stream()
                .map(tableName -> getTable(databaseName, tableName))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .filter(ViewReaderUtil::isSomeKindOfAView)
                .map(Table::getTableName)
                .collect(toImmutableList());
    }

    @Override
    public Optional<List<SchemaTableName>> getAllViews()
    {
        return Optional.empty();
    }

    @Override
    public synchronized void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Table table = getRequiredTable(databaseName, tableName);

        Location tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

        if (deleteData) {
            deleteDirectoryAndSchema(TABLE, tableMetadataDirectory);
        }
        else {
            deleteSchemaFile(TABLE, tableMetadataDirectory);
            deleteTablePrivileges(table);
        }
    }

    @Override
    public synchronized void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges)
    {
        Table table = getRequiredTable(databaseName, tableName);
        if (!table.getDatabaseName().equals(databaseName) || !table.getTableName().equals(tableName)) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Replacement table must have same name");
        }

        if (isIcebergTable(table) && !Objects.equals(table.getParameters().get("metadata_location"), newTable.getParameters().get("previous_metadata_location"))) {
            throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED, "Cannot update Iceberg table: supplied previous location does not match current location");
        }

        Location tableMetadataDirectory = getTableMetadataDirectory(table);
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
    public synchronized void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(newDatabaseName, "newDatabaseName is null");
        requireNonNull(newTableName, "newTableName is null");

        Table table = getRequiredTable(databaseName, tableName);
        getRequiredDatabase(newDatabaseName);

        // verify new table does not exist
        verifyTableNameLength(newTableName);
        verifyTableNotExists(newDatabaseName, newTableName);

        Location oldPath = getTableMetadataDirectory(databaseName, tableName);
        Location newPath = getTableMetadataDirectory(newDatabaseName, newTableName);

        try {
            if (isIcebergTable(table)) {
                fileSystem.createDirectory(newPath);
                // Iceberg metadata references files in old path, so these cannot be moved. Moving table description (metadata from metastore perspective) only.
                fileSystem.renameFile(getSchemaFile(TABLE, oldPath), getSchemaFile(TABLE, newPath));
                // TODO drop data files when table is being dropped
            }
            else {
                fileSystem.renameDirectory(oldPath, newPath);
            }
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
        finally {
            listTablesCache.invalidateAll();
        }
    }

    @Override
    public synchronized void commentTable(String databaseName, String tableName, Optional<String> comment)
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
    public synchronized void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(columnName).isEmpty()) {
                SchemaTableName name = new SchemaTableName(databaseName, tableName);
                throw new ColumnNotFoundException(name, columnName);
            }

            ImmutableList.Builder<Column> newDataColumns = ImmutableList.builder();
            for (Column fieldSchema : oldTable.getDataColumns()) {
                if (fieldSchema.getName().equals(columnName)) {
                    newDataColumns.add(new Column(columnName, fieldSchema.getType(), comment, fieldSchema.getProperties()));
                }
                else {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(currentVersion, newDataColumns.build());
        });
    }

    @Override
    public synchronized void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        alterTable(databaseName, tableName, oldTable -> {
            if (oldTable.getColumn(columnName).isPresent()) {
                throw new TrinoException(ALREADY_EXISTS, "Column already exists: " + columnName);
            }

            return oldTable.withDataColumns(
                    currentVersion,
                    ImmutableList.<Column>builder()
                            .addAll(oldTable.getDataColumns())
                            .add(new Column(columnName, columnType, Optional.ofNullable(columnComment), ImmutableMap.of()))
                            .build());
        });
    }

    @Override
    public synchronized void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
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
                    newDataColumns.add(new Column(newColumnName, fieldSchema.getType(), fieldSchema.getComment(), fieldSchema.getProperties()));
                }
                else {
                    newDataColumns.add(fieldSchema);
                }
            }

            return oldTable.withDataColumns(currentVersion, newDataColumns.build());
        });
    }

    @Override
    public synchronized void dropColumn(String databaseName, String tableName, String columnName)
    {
        alterTable(databaseName, tableName, oldTable -> {
            verifyCanDropColumn(this, databaseName, tableName, columnName);
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

        Location tableMetadataDirectory = getTableMetadataDirectory(databaseName, tableName);

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
    public synchronized void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitions, "partitions is null");

        Table table = getRequiredTable(databaseName, tableName);

        TableType tableType = TableType.valueOf(table.getTableType());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE).contains(tableType), "Invalid table type: %s", tableType);

        try {
            Map<Location, byte[]> schemaFiles = new LinkedHashMap<>();
            for (PartitionWithStatistics partitionWithStatistics : partitions) {
                Partition partition = partitionWithStatistics.getPartition();
                verifiedPartition(table, partition);
                Location partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
                Location schemaPath = getSchemaFile(PARTITION, partitionMetadataDirectory);

                if (fileSystem.directoryExists(schemaPath).orElse(false)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Partition already exists");
                }
                byte[] schemaJson = partitionCodec.toJsonBytes(new PartitionMetadata(table, partitionWithStatistics));
                schemaFiles.put(schemaPath, schemaJson);
            }

            Set<Location> createdFiles = new LinkedHashSet<>();
            try {
                for (Entry<Location, byte[]> entry : schemaFiles.entrySet()) {
                    try (OutputStream outputStream = fileSystem.newOutputFile(entry.getKey()).create()) {
                        createdFiles.add(entry.getKey());
                        outputStream.write(entry.getValue());
                    }
                    catch (IOException e) {
                        throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write partition schema", e);
                    }
                }
            }
            catch (Throwable e) {
                try {
                    fileSystem.deleteFiles(createdFiles);
                }
                catch (IOException ex) {
                    if (!e.equals(ex)) {
                        e.addSuppressed(ex);
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
        Location partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());

        if (table.getTableType().equals(MANAGED_TABLE.name())) {
            if (!partitionMetadataDirectory.equals(Location.of(partition.getStorage().getLocation()))) {
                throw new TrinoException(HIVE_METASTORE_ERROR, "Partition directory must be " + partitionMetadataDirectory);
            }
        }
        else if (table.getTableType().equals(EXTERNAL_TABLE.name())) {
            try {
                Location externalLocation = Location.of(partition.getStorage().getLocation());
                if (!fileSystem.directoryExists(externalLocation).orElse(true)) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, "External partition location does not exist");
                }
                if (externalLocation.toString().startsWith(catalogDirectory.toString())) {
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
    public synchronized void dropPartition(String databaseName, String tableName, List<String> partitionValues, boolean deleteData)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (tableReference.isEmpty()) {
            return;
        }
        Table table = tableReference.get();

        Location partitionMetadataDirectory = getPartitionMetadataDirectory(table, partitionValues);
        if (deleteData) {
            deleteDirectoryAndSchema(PARTITION, partitionMetadataDirectory);
        }
        else {
            deleteSchemaFile(PARTITION, partitionMetadataDirectory);
        }
    }

    @Override
    public synchronized void alterPartition(String databaseName, String tableName, PartitionWithStatistics partitionWithStatistics)
    {
        Table table = getRequiredTable(databaseName, tableName);

        Partition partition = partitionWithStatistics.getPartition();
        verifiedPartition(table, partition);

        Location partitionMetadataDirectory = getPartitionMetadataDirectory(table, partition.getValues());
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
        Map<RoleGrantee, RoleGrant> map = new HashMap<>();
        for (RoleGrant grant : grants) {
            RoleGrantee tuple = new RoleGrantee(grant.getRoleName(), HivePrincipal.from(grant.getGrantee()));
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

    private synchronized Optional<List<String>> getAllPartitionNames(String databaseName, String tableName)
    {
        requireNonNull(databaseName, "databaseName is null");
        requireNonNull(tableName, "tableName is null");

        Optional<Table> tableReference = getTable(databaseName, tableName);
        if (tableReference.isEmpty()) {
            return Optional.empty();
        }
        Table table = tableReference.get();

        Location tableMetadataDirectory = getTableMetadataDirectory(table);

        List<List<String>> partitions = listPartitions(tableMetadataDirectory, table.getPartitionColumns());

        List<String> partitionNames = partitions.stream()
                .map(partitionValues -> makePartitionName(table.getPartitionColumns(), ImmutableList.copyOf(partitionValues)))
                .filter(partitionName -> isValidPartition(table, partitionName))
                .collect(toImmutableList());

        return Optional.of(partitionNames);
    }

    private boolean isValidPartition(Table table, String partitionName)
    {
        Location location = getSchemaFile(PARTITION, getPartitionMetadataDirectory(table, partitionName));
        try {
            return fileSystem.newInputFile(location).exists();
        }
        catch (IOException e) {
            return false;
        }
    }

    private List<List<String>> listPartitions(Location directory, List<io.trino.plugin.hive.metastore.Column> partitionColumns)
    {
        if (partitionColumns.isEmpty()) {
            return ImmutableList.of();
        }

        try {
            List<List<String>> partitionValues = new ArrayList<>();
            FileIterator iterator = fileSystem.listFiles(directory);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();
                String path = location.toString().substring(directory.toString().length());

                if (path.startsWith("/")) {
                    path = path.substring(1);
                }

                if (!path.endsWith("/" + TRINO_SCHEMA_FILE_NAME_SUFFIX)) {
                    continue;
                }
                path = path.substring(0, path.length() - TRINO_SCHEMA_FILE_NAME_SUFFIX.length() - 1);

                List<String> values = toPartitionValues(path);
                if (values.size() == partitionColumns.size()) {
                    partitionValues.add(values);
                }
            }
            return partitionValues;
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Error listing partition directories", e);
        }
    }

    @Override
    public synchronized Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        requireNonNull(table, "table is null");
        requireNonNull(partitionValues, "partitionValues is null");

        Location partitionDirectory = getPartitionMetadataDirectory(table, partitionValues);
        return readSchemaFile(PARTITION, partitionDirectory, partitionCodec)
                .map(partitionMetadata -> partitionMetadata.toPartition(table.getDatabaseName(), table.getTableName(), partitionValues, partitionDirectory.toString()));
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(
            String databaseName,
            String tableName,
            List<String> columnNames,
            TupleDomain<String> partitionKeysFilter)
    {
        return getAllPartitionNames(databaseName, tableName);
    }

    @Override
    public synchronized Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        ImmutableMap.Builder<String, Optional<Partition>> builder = ImmutableMap.builder();
        for (String partitionName : partitionNames) {
            List<String> partitionValues = toPartitionValues(partitionName);
            builder.put(partitionName, getPartition(table, partitionValues));
        }
        return builder.buildOrThrow();
    }

    @Override
    public synchronized Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        Table table = getRequiredTable(databaseName, tableName);
        Location permissionsDirectory = getPermissionsDirectory(table);
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

            Location permissionsDirectory = getPermissionsDirectory(table);

            fileSystem.createDirectory(permissionsDirectory);

            Location permissionFilePath = getPermissionsPath(permissionsDirectory, grantee);
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
            Location permissionsDirectory = getPermissionsDirectory(table);
            fileSystem.deleteDirectory(permissionsDirectory);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete table permissions", e);
        }
    }

    private Set<HivePrivilegeInfo> readPermissionsFile(Location permissionFilePath)
    {
        return readFile("permissions", permissionFilePath, permissionsCodec).orElse(ImmutableList.of()).stream()
                .map(PermissionMetadata::toHivePrivilegeInfo)
                .collect(toImmutableSet());
    }

    private Set<HivePrivilegeInfo> readAllPermissions(Location permissionsDirectory)
    {
        try {
            ImmutableSet.Builder<HivePrivilegeInfo> permissions = ImmutableSet.builder();
            FileIterator iterator = fileSystem.listFiles(permissionsDirectory);
            while (iterator.hasNext()) {
                Location location = iterator.next().location();
                if (!location.fileName().startsWith(".")) {
                    permissions.addAll(readPermissionsFile(location));
                }
            }
            return permissions.build();
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, e);
        }
    }

    private void deleteDirectoryAndSchema(SchemaType type, Location metadataDirectory)
    {
        try {
            Location schemaPath = getSchemaFile(type, metadataDirectory);
            if (!fileSystem.newInputFile(schemaPath).exists()) {
                // if there is no schema file, assume this is not a database, partition or table
                return;
            }

            // Delete the schema file first, so it can never exist after the directory is deleted.
            // (For cases when the schema file isn't in the metadata directory.)
            deleteSchemaFile(type, metadataDirectory);

            fileSystem.deleteDirectory(metadataDirectory);
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

    private <T> Optional<T> readSchemaFile(SchemaType type, Location metadataDirectory, JsonCodec<T> codec)
    {
        return readFile(type + " schema", getSchemaFile(type, metadataDirectory), codec);
    }

    private <T> Optional<T> readFile(String type, Location file, JsonCodec<T> codec)
    {
        try {
            try (InputStream inputStream = fileSystem.newInputFile(file).newStream()) {
                byte[] json = ByteStreams.toByteArray(inputStream);
                return Optional.of(codec.fromJson(json));
            }
        }
        catch (FileNotFoundException e) {
            return Optional.empty();
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not read " + type, e);
        }
    }

    private <T> void writeSchemaFile(SchemaType type, Location directory, JsonCodec<T> codec, T value, boolean overwrite)
    {
        writeFile(type + " schema", getSchemaFile(type, directory), codec, value, overwrite);
    }

    private <T> void writeFile(String type, Location location, JsonCodec<T> codec, T value, boolean overwrite)
    {
        try {
            byte[] json = codec.toJsonBytes(value);

            if (!overwrite) {
                if (fileSystem.newInputFile(location).exists()) {
                    throw new TrinoException(HIVE_METASTORE_ERROR, type + " file already exists");
                }
            }

            // todo implement safer overwrite code
            TrinoOutputFile output = fileSystem.newOutputFile(location);
            try (OutputStream outputStream = overwrite ? output.createOrOverwrite() : output.create()) {
                outputStream.write(json);
            }
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not write " + type, e);
        }
        finally {
            listTablesCache.invalidateAll();
        }
    }

    private void renameSchemaFile(SchemaType type, Location oldMetadataDirectory, Location newMetadataDirectory)
    {
        try {
            fileSystem.renameFile(getSchemaFile(type, oldMetadataDirectory), getSchemaFile(type, newMetadataDirectory));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not rename " + type + " schema", e);
        }
        finally {
            listTablesCache.invalidateAll();
        }
    }

    private void deleteSchemaFile(SchemaType type, Location metadataDirectory)
    {
        try {
            fileSystem.deleteFile(getSchemaFile(type, metadataDirectory));
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Could not delete " + type + " schema", e);
        }
        finally {
            listTablesCache.invalidateAll();
        }
    }

    private Location getDatabaseMetadataDirectory(String databaseName)
    {
        return catalogDirectory.appendPath(escapeSchemaName(databaseName));
    }

    private Location getTableMetadataDirectory(Table table)
    {
        return getTableMetadataDirectory(table.getDatabaseName(), table.getTableName());
    }

    private Location getTableMetadataDirectory(String databaseName, String tableName)
    {
        return getDatabaseMetadataDirectory(databaseName).appendPath(escapeTableName(tableName));
    }

    private Location getPartitionMetadataDirectory(Table table, List<String> values)
    {
        String partitionName = makePartitionName(table.getPartitionColumns(), values);
        return getPartitionMetadataDirectory(table, partitionName);
    }

    private Location getPartitionMetadataDirectory(Table table, String partitionName)
    {
        return getTableMetadataDirectory(table).appendPath(partitionName);
    }

    private Location getPermissionsDirectory(Table table)
    {
        return getTableMetadataDirectory(table).appendPath(TRINO_PERMISSIONS_DIRECTORY_NAME);
    }

    private static Location getPermissionsPath(Location permissionsDirectory, HivePrincipal grantee)
    {
        String granteeType = grantee.getType().toString().toLowerCase(Locale.US);
        return permissionsDirectory.appendPath(granteeType + "_" + grantee.getName());
    }

    private Location getRolesFile()
    {
        return catalogDirectory.appendPath(ROLES_FILE_NAME);
    }

    private Location getRoleGrantsFile()
    {
        return catalogDirectory.appendPath(ROLE_GRANTS_FILE_NAME);
    }

    private static Location getSchemaFile(SchemaType type, Location metadataDirectory)
    {
        if (type == DATABASE) {
            String path = metadataDirectory.toString();
            if (path.endsWith("/")) {
                path = path.substring(0, path.length() - 1);
            }
            checkArgument(!path.isEmpty(), "Can't use root directory as database path: %s", metadataDirectory);
            int index = path.lastIndexOf('/');
            if (index >= 0) {
                path = path.substring(0, index + 1) + "." + path.substring(index + 1);
            }
            else {
                path = "." + path;
            }
            return Location.of(path).appendSuffix(TRINO_SCHEMA_FILE_NAME_SUFFIX);
        }
        return metadataDirectory.appendPath(TRINO_SCHEMA_FILE_NAME_SUFFIX);
    }

    private record RoleGrantee(String role, HivePrincipal grantee)
    {
        private RoleGrantee
        {
            requireNonNull(role, "role is null");
            requireNonNull(grantee, "grantee is null");
        }
    }

    // Visible to allow import into this file
    enum SchemaType
    {
        DATABASE, TABLE, PARTITION;

        @Override
        public String toString()
        {
            return name().toLowerCase(ENGLISH);
        }
    }
}
