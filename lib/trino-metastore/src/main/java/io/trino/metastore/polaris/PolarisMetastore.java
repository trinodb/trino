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
package io.trino.metastore.polaris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
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
import io.trino.metastore.Storage;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.rest.auth.OAuth2Properties;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.metastore.MetastoreErrorCode.HIVE_CONCURRENT_MODIFICATION_DETECTED;
import static io.trino.metastore.MetastoreErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class PolarisMetastore
        implements HiveMetastore
{
    private final PolarisRestClient polarisClient;
    private final RESTSessionCatalog restSessionCatalog;
    private final SecurityProperties securityProperties;
    private final PolarisMetastoreStats stats;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public PolarisMetastore(PolarisRestClient polarisClient, RESTSessionCatalog restSessionCatalog, SecurityProperties securityProperties, PolarisMetastoreStats stats, TrinoFileSystemFactory fileSystemFactory)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
        this.securityProperties = requireNonNull(securityProperties, "securityProperties is null");
        this.stats = requireNonNull(stats, "stats is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    public PolarisMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        stats.getGetDatabase().update(1);
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            List<Namespace> namespaces = restSessionCatalog.listNamespaces(sessionContext, Namespace.empty());

            Optional<Namespace> namespace = namespaces.stream()
                    .filter(ns -> ns.toString().equals(databaseName))
                    .findFirst();

            if (namespace.isPresent()) {
                Map<String, String> properties = restSessionCatalog.loadNamespaceMetadata(sessionContext, namespace.get());
                Optional<String> location = Optional.ofNullable(properties.get("location"));

                Database database = Database.builder()
                        .setDatabaseName(namespace.get().toString())
                        .setLocation(location)
                        .setOwnerName(Optional.of("trino-user"))
                        .setOwnerType(Optional.of(PrincipalType.USER))
                        .setParameters(properties)
                        .build();

                return Optional.of(database);
            }
            return Optional.empty();
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to get database: " + databaseName, e);
        }
    }

    @Override
    public List<String> getAllDatabases()
    {
        stats.getGetAllDatabases().update(1);
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            List<Namespace> namespaces = restSessionCatalog.listNamespaces(sessionContext, Namespace.empty());

            return namespaces.stream()
                    .map(Namespace::toString)
                    .collect(toImmutableList());
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list databases", e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        stats.getGetTable().update(1);
        try {
            TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            stats.getLoadIcebergTable().update(1);
            org.apache.iceberg.BaseTable icebergTable = (org.apache.iceberg.BaseTable) restSessionCatalog.loadTable(sessionContext, tableId);

            Table hiveTable = convertIcebergToHiveTable(databaseName, tableName, icebergTable);
            return Optional.of(hiveTable);
        }
        catch (Exception e) {
            try {
                stats.getLoadGenericTable().update(1);
                PolarisGenericTable genericTable = polarisClient.loadGenericTable(databaseName, tableName);
                Table result = convertGenericToHiveTable(databaseName, genericTable);
                return Optional.of(result);
            }
            catch (Exception ex) {
                return Optional.empty();
            }
        }
    }

    /**
     * Creates session context using the same credentials as RESTSessionCatalog
     * This ensures consistent OAuth2 authentication across all operations
     */
    private SessionCatalog.SessionContext createSessionContext()
    {
        String sessionId = "trino-polaris-hive-session";

        Map<String, String> securityProps = securityProperties.get();
        Map<String, String> credentials = ImmutableMap.<String, String>builder()
                .putAll(Maps.filterKeys(securityProps,
                    key -> Set.of(OAuth2Properties.TOKEN, OAuth2Properties.CREDENTIAL).contains(key)))
                .buildOrThrow();

        Map<String, String> properties = ImmutableMap.of();

        return new SessionCatalog.SessionContext(sessionId, "trino-user", credentials, properties, null);
    }

    private String translatePathForContainer(String hostMetadataLocation)
    {
        if (!hostMetadataLocation.startsWith("file://")) {
            return hostMetadataLocation;
        }

        String hostFilePath = hostMetadataLocation.substring(7);

        if (hostFilePath.contains("/var/folders/") && hostFilePath.contains("/T/polaris-warehouse")) {
            String containerPath = "file://" + hostFilePath;
            return containerPath;
        }
        return hostMetadataLocation;
    }

    public Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        return ImmutableSet.of();
    }

    public PartitionStatistics getTableStatistics(String databaseName, String tableName)
    {
        return PartitionStatistics.empty();
    }

    public Map<String, PartitionStatistics> getPartitionStatistics(String databaseName, String tableName, Set<String> partitionNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public void updateTableStatistics(String databaseName, String tableName, OptionalLong acidWriteId, StatisticsUpdateMode mode, PartitionStatistics statisticsUpdate)
    {
        // Polaris doesn't support statistics updates, but we should not fail table creation because of this
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        // Polaris doesn't support statistics updates, but we should not fail operations because of this
    }

    public List<String> getAllTables(String databaseName)
    {
        stats.getGetAllTables().update(1);
        try {
            ImmutableList.Builder<String> allTables = ImmutableList.builder();

            try {
                stats.getListIcebergTables().update(1);
                SessionCatalog.SessionContext sessionContext = createSessionContext();
                Namespace namespace = Namespace.of(databaseName.split("\\."));
                List<String> icebergTables = restSessionCatalog.listTables(sessionContext, namespace)
                        .stream()
                        .map(TableIdentifier::name)
                        .collect(toImmutableList());
                allTables.addAll(icebergTables);
            }
            catch (RuntimeException e) {
                // Don't fail - database might not contain Iceberg tables
            }

            try {
                stats.getListGenericTables().update(1);
                List<String> genericTables = polarisClient.listGenericTables(databaseName)
                        .stream()
                        .map(PolarisTableIdentifier::name)
                        .collect(toImmutableList());
                allTables.addAll(genericTables);
            }
            catch (RuntimeException e) {
                // Don't fail - database might not contain generic tables
            }

            return allTables.build();
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list tables in database: " + databaseName, e);
        }
    }

    @Override
    public List<TableInfo> getTables(String databaseName)
    {
        return getAllTables(databaseName).stream()
                .map(tableName -> new TableInfo(new SchemaTableName(databaseName, tableName), TableInfo.ExtendedRelationType.TABLE))
                .collect(toImmutableList());
    }

    @Override
    public List<String> getTableNamesWithParameters(String databaseName, String parameterKey, Set<String> parameterValues)
    {
        return ImmutableList.of();
    }

    public List<String> getAllViews(String databaseName)
    {
        // TODO: implement view support, the Polaris API supports it.
        return ImmutableList.of();
    }

    @Override
    public void createDatabase(Database database)
    {
        stats.getCreateDatabase().update(1);

        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace namespace = Namespace.of(database.getDatabaseName());
            restSessionCatalog.createNamespace(sessionContext, namespace, database.getParameters());

            Map<String, String> namespaceProperties = restSessionCatalog.loadNamespaceMetadata(sessionContext, namespace);
            String polarisLocation = namespaceProperties.get("location");

            if (polarisLocation != null) {
                Location location = Location.of(polarisLocation);
                try {
                    TrinoFileSystem fileSystem = fileSystemFactory.create(io.trino.spi.security.ConnectorIdentity.ofUser("system"));
                    fileSystem.createDirectory(location);
                }
                catch (Exception e) {
                    try {
                        restSessionCatalog.dropNamespace(sessionContext, namespace);
                    }
                    catch (Exception cleanupException) {
                        e.addSuppressed(cleanupException);
                    }
                    throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create directory: " + location, e);
                }
            }
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create database: " + database.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            Namespace namespace = Namespace.of(databaseName);
            restSessionCatalog.dropNamespace(sessionContext, namespace);
        }
        catch (NoSuchNamespaceException e) {
            throw new SchemaNotFoundException(databaseName, e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop database: " + databaseName, e);
        }
    }

    @Override
    public void renameDatabase(String databaseName, String newDatabaseName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Rename database is not supported by Polaris");
    }

    @Override
    public void setDatabaseOwner(String databaseName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Database ownership is not supported by Polaris");
    }

    @Override
    public void createTable(Table table, PrincipalPrivileges principalPrivileges)
    {
        stats.getCreateTable().update(1);
        String tableFormat = detectTableFormat(table);

        if ("ICEBERG".equals(tableFormat)) {
            createIcebergTable(table);
            return;
        }
        if ("DELTA".equals(tableFormat)) {
            createGenericTable(table);
            return;
        }

        throw new TrinoException(NOT_SUPPORTED,
                "Table creation through metastore is not supported for table type: " + tableFormat +
                ". Table: " + table.getDatabaseName() + "." + table.getTableName());
    }

    private void createIcebergTable(Table table)
    {
        String metadataLocation = table.getParameters().get("metadata_location");

        if (metadataLocation != null) {
            try {
                SessionCatalog.SessionContext sessionContext = createSessionContext();
                TableIdentifier tableId = TableIdentifier.of(table.getDatabaseName(), table.getTableName());

                String containerMetadataLocation = translatePathForContainer(metadataLocation);

                restSessionCatalog.registerTable(sessionContext, tableId, containerMetadataLocation);
                stats.getCreateIcebergTable().update(1);
                return;
            }
            catch (Exception e) {
                throw new TrinoException(HIVE_METASTORE_ERROR,
                        "Failed to register Iceberg table with Polaris: " + table.getDatabaseName()
                                + "." + table.getTableName(), e);
            }
        }
        else {
            throw new TrinoException(HIVE_METASTORE_ERROR,
                    "Iceberg table missing metadata_location parameter: " + table.getDatabaseName() + "." + table.getTableName());
        }
    }

    private void createGenericTable(Table table)
    {
        try {
            PolarisGenericTable genericTable = convertTableToGenericTable(table);

            polarisClient.createGenericTable(table.getDatabaseName(), genericTable);
            stats.getCreateGenericTable().update(1);
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR,
                    "Failed to register generic table in Polaris: " + table.getDatabaseName() + "." + table.getTableName(), e);
        }
    }

    private PolarisGenericTable convertTableToGenericTable(Table table)
    {
        Map<String, String> properties = new HashMap<>(table.getParameters());

        Storage storage = table.getStorage();
        if (storage.getLocation() != null) {
            properties.put("location", storage.getLocation());
        }
        if (storage.getStorageFormat().getInputFormat() != null) {
            properties.put("input_format", storage.getStorageFormat().getInputFormat());
        }
        if (storage.getStorageFormat().getOutputFormat() != null) {
            properties.put("output_format", storage.getStorageFormat().getOutputFormat());
        }
        if (storage.getStorageFormat().getSerde() != null) {
            properties.put("serde", storage.getStorageFormat().getSerde());
        }

        String format = "GENERIC";
        if ("delta".equalsIgnoreCase(table.getParameters().get("spark.sql.sources.provider"))) {
            format = "DELTA";
        }

        return new PolarisGenericTable(
                table.getTableName(),
                format,
                Optional.ofNullable(storage.getLocation()),
                Optional.ofNullable(table.getParameters().get("comment")),
                properties);
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        stats.getReplaceTable().update(1);

        try {
            String tableFormat = detectTableFormat(newTable);

            switch (tableFormat) {
                case "ICEBERG" -> {
                    stats.getReplaceIcebergTable().update(1);
                    replaceIcebergTable(databaseName, tableName, newTable);
                }
                case "DELTA" -> {
                    stats.getReplaceGenericTable().update(1);
                    replaceGenericTable(databaseName, tableName, newTable);
                }
                default -> {
                    Optional<Table> existingTable = getTable(databaseName, tableName);
                    if (existingTable.isEmpty()) {
                        throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                    }
                }
            }
        }
        catch (TableNotFoundException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to replace table: " + databaseName + "." + tableName, e);
        }
    }

    private void replaceIcebergTable(String databaseName, String tableName, Table newTable)
    {
        String newMetadataLocation = newTable.getParameters().get("metadata_location");
        String previousMetadataLocation = newTable.getParameters().get("previous_metadata_location");

        if (newMetadataLocation == null) {
            throw new TrinoException(HIVE_METASTORE_ERROR,
                    "Missing metadata_location parameter for Iceberg table update: " + databaseName + "." + tableName);
        }

        try {
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);

            if (previousMetadataLocation != null) {
                Optional<Table> currentTable = getTable(databaseName, tableName);
                if (currentTable.isEmpty()) {
                    throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                }

                String currentMetadataLocation = currentTable.get().getParameters().get("metadata_location");
                if (!Objects.equals(currentMetadataLocation, previousMetadataLocation)) {
                    throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED,
                            "Cannot update Iceberg table: supplied previous location does not match current location. " +
                            "Expected: " + previousMetadataLocation + ", but found: " + currentMetadataLocation);
                }
            }

            // Attempt atomic update using drop-and-recreate with validation
            // Note: RESTSessionCatalog doesn't expose atomic updateTable, so we use validated drop-and-recreate
            // TODO: Make this completely atomic
            try {
                restSessionCatalog.dropTable(sessionContext, tableId);
                restSessionCatalog.registerTable(sessionContext, tableId, newMetadataLocation);
            }
            catch (Exception updateException) {
                Optional<Table> currentTable = getTable(databaseName, tableName);
                if (currentTable.isPresent()) {
                    String currentMetadataLocation = currentTable.get().getParameters().get("metadata_location");
                    if (previousMetadataLocation != null && !Objects.equals(currentMetadataLocation, previousMetadataLocation)) {
                        throw new TrinoException(HIVE_CONCURRENT_MODIFICATION_DETECTED,
                                "Concurrent modification detected during Iceberg table update. " +
                                "Expected previous location: " + previousMetadataLocation +
                                ", but current location is: " + currentMetadataLocation);
                    }
                }
                throw updateException;
            }
        }
        catch (TrinoException e) {
            throw e;
        }
        catch (Exception e) {
            throw new TrinoException(HIVE_METASTORE_ERROR,
                    "Failed to update Iceberg table in Polaris: " + databaseName + "." + tableName, e);
        }
    }

    private void replaceGenericTable(String databaseName, String tableName, Table newTable)
    {
        // Generic table updates are not supported by Polaris REST catalog
        // Users must drop and recreate generic tables to update them
        throw new TrinoException(NOT_SUPPORTED,
                "Updating generic tables is not supported by Polaris yet. " +
                "To update table '" + databaseName + "." + tableName + "', with " + newTable.getParameters() +
                "please drop the table and recreate it with the new configuration.");
    }

    /**
     * Simple table format detection for  purposes
     * The connector has already determined the format and set appropriate parameters
     */
    String detectTableFormat(Table table)
    {
        Map<String, String> parameters = table.getParameters();

        if ("iceberg".equalsIgnoreCase(parameters.get("table_type")) ||
                "iceberg".equalsIgnoreCase(parameters.get("table_format")) ||
                parameters.containsKey("metadata_location")) {
            return "ICEBERG";
        }

        if ("delta".equalsIgnoreCase(parameters.get("spark.sql.sources.provider"))) {
            return "DELTA";
        }

        // TODO: Check for additional table indicators

        throw new TrinoException(NOT_SUPPORTED,
        "Unable to determine table format for table: " +
        table.getDatabaseName() + "." + table.getTableName() +
        ". Parameters: " + parameters);
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        stats.getDropTable().update(1);
        try {
            Optional<Table> existingTable = getTable(databaseName, tableName);
            if (existingTable.isEmpty()) {
                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
            }

            String tableFormat = detectTableFormat(existingTable.get());

            switch (tableFormat) {
                case "ICEBERG" -> {
                    stats.getDropIcebergTable().update(1);
                    dropIcebergTable(databaseName, tableName, deleteData);
                }
                case "DELTA" -> {
                    stats.getDropGenericTable().update(1);
                    polarisClient.dropGenericTable(databaseName, tableName);
                }
                default -> {
                    throw new TrinoException(NOT_SUPPORTED, "Unsupported table format for drop: " + tableFormat);
                }
            }
        }
        catch (TableNotFoundException e) {
            throw e;
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop table: " + databaseName + "." + tableName, e);
        }
    }

    private void dropIcebergTable(String databaseName, String tableName, boolean deleteData)
    {
        SessionCatalog.SessionContext sessionContext = createSessionContext();
        TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);

        if (deleteData) {
            try {
                restSessionCatalog.purgeTable(sessionContext, tableId);
            }
            catch (ForbiddenException e) {
                restSessionCatalog.dropTable(sessionContext, tableId);
            }
        }
        else {
            restSessionCatalog.dropTable(sessionContext, tableId);
        }
    }

    private Table convertIcebergToHiveTable(String databaseName, String tableName, org.apache.iceberg.BaseTable icebergTable)
    {
        List<Column> dummyColumns = ImmutableList.of(
                new Column("dummy_col", HiveType.HIVE_STRING, Optional.empty(), ImmutableMap.of()));

        Map<String, String> parameters = ImmutableMap.<String, String>builder()
                .put("table_type", "iceberg")
                .put("metadata_location", icebergTable.operations().current().metadataFileLocation())
                .buildOrThrow();

        return Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setOwner(Optional.of("trino-user"))
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(dummyColumns)
                .setParameters(parameters)
                .withStorage(storage -> storage
                        .setLocation(icebergTable.location())
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                                "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
                        .setSerdeParameters(ImmutableMap.of()))
                .build();
    }

    private Table convertGenericToHiveTable(String databaseName, PolarisGenericTable genericTable)
    {
        List<Column> columns;
        if ("delta".equalsIgnoreCase(genericTable.format())) {
            columns = ImmutableList.of(
                    new Column("id", HiveType.HIVE_INT, Optional.empty(), ImmutableMap.of()),
                    new Column("name", HiveType.HIVE_STRING, Optional.empty(), ImmutableMap.of()),
                    new Column("value", HiveType.HIVE_STRING, Optional.empty(), ImmutableMap.of()));
        }
        else {
            columns = ImmutableList.of();
        }

        StorageFormat storageFormat;
        if ("delta".equalsIgnoreCase(genericTable.format())) {
            storageFormat = StorageFormat.create(
                    "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
                    "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat");
        }
        else {
            storageFormat = switch (genericTable.format().toLowerCase(Locale.ROOT)) {
                case "csv" -> StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.OpenCSVSerde",
                        "org.apache.hadoop.mapred.TextInputFormat",
                        "org.apache.hadoop.mapred.HiveIgnoreKeyTextOutputFormat");
                default -> StorageFormat.create(
                        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
                        "org.apache.hadoop.mapred.FileInputFormat",
                        "org.apache.hadoop.mapred.FileOutputFormat");
            };
        }

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        genericTable.properties().entrySet().stream()
                .filter(entry -> !isDeltaSpecificProperty(entry.getKey()))
                .forEach(entry -> parameters.put(entry.getKey(), entry.getValue()));

        genericTable.doc().ifPresent(doc -> parameters.put("comment", doc));

        if ("delta".equalsIgnoreCase(genericTable.format())) {
            parameters.put("spark.sql.sources.provider", "delta");

            String tableLocation = null;
            if (genericTable.baseLocation().isPresent()) {
                tableLocation = genericTable.baseLocation().get();
            }
            else if (genericTable.properties().containsKey("location")) {
                tableLocation = genericTable.properties().get("location");
            }

            if (tableLocation != null) {
                parameters.put("path", tableLocation);
            }
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(genericTable.name())
                .setOwner(Optional.of("trino-user"))
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(columns)
                .setParameters(parameters.buildOrThrow());

        tableBuilder.withStorage(storage -> {
            storage.setStorageFormat(storageFormat);

            Map<String, String> serdeParams = new HashMap<>();
            if ("delta".equalsIgnoreCase(genericTable.format())) {
                String tableLocation = null;
                if (genericTable.baseLocation().isPresent()) {
                    tableLocation = genericTable.baseLocation().get();
                }
                else if (genericTable.properties().containsKey("location")) {
                    tableLocation = genericTable.properties().get("location");
                }

                if (tableLocation != null) {
                    serdeParams.put("path", tableLocation);
                }
            }
            ImmutableMap<String, String> finalSerdeParams = ImmutableMap.copyOf(serdeParams);
            storage.setSerdeParameters(finalSerdeParams);

            if (genericTable.baseLocation().isPresent()) {
                String location = genericTable.baseLocation().get();
                storage.setLocation(location);
            }
            else if (genericTable.properties().containsKey("location")) {
                String location = genericTable.properties().get("location");
                storage.setLocation(location);
            }
        });

        Table result = tableBuilder.build();
        return result;
    }

    /**
     * Check if a property is Delta-specific and should be filtered out
     */
    private boolean isDeltaSpecificProperty(String key)
    {
        return key.equals("format") ||
               key.equals("spark.sql.sources.provider") ||
               key.startsWith("delta.") ||
               key.equals("table_type") ||
               key.equals("has_delta_log");
    }

    // Additional HiveMetastore methods - Most are not supported by Polaris
    @Override
    public Map<String, HiveColumnStatistics> getTableColumnStatistics(String databaseName, String tableName, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public Map<String, Map<String, HiveColumnStatistics>> getPartitionColumnStatistics(String databaseName, String tableName, Set<String> partitionNames, Set<String> columnNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table rename is not supported by Polaris");
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table comment updates are not supported by Polaris");
    }

    @Override
    public void setTableOwner(String databaseName, String tableName, HivePrincipal principal)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table ownership is not supported by Polaris");
    }

    @Override
    public void commentColumn(String databaseName, String tableName, String columnName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column comments are not supported by Polaris");
    }

    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column addition is not supported by Polaris ");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column rename is not supported by Polaris ");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Column drop is not supported by Polaris ");
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    @Override
    public Optional<List<String>> getPartitionNamesByFilter(String databaseName, String tableName, List<String> columnNames, TupleDomain<String> partitionKeysFilter)
    {
        return Optional.empty();
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        return ImmutableMap.of();
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<PartitionWithStatistics> partitions)
    {
        throw new TrinoException(NOT_SUPPORTED, "Partition operations are not supported by Polaris ");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "Partition operations are not supported by Polaris ");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new TrinoException(NOT_SUPPORTED, "Partition operations are not supported by Polaris ");
    }

    @Override
    public void createRole(String role, String grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Role management is not supported by Polaris ");
    }

    @Override
    public void dropRole(String role)
    {
        throw new TrinoException(NOT_SUPPORTED, "Role management is not supported by Polaris ");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Role management is not supported by Polaris ");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new TrinoException(NOT_SUPPORTED, "Role management is not supported by Polaris ");
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "Privilege management is not supported by Polaris ");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilege> privileges, boolean grantOption)
    {
        throw new TrinoException(NOT_SUPPORTED, "Privilege management is not supported by Polaris ");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return ImmutableList.of();
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return ImmutableList.of();
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "Function management is not supported by Polaris ");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new TrinoException(NOT_SUPPORTED, "Function management is not supported by Polaris ");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new TrinoException(NOT_SUPPORTED, "Function management is not supported by Polaris ");
    }
}
