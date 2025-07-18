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
package io.trino.plugin.hive.metastore.polaris;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.trino.metastore.AcidTransactionOwner;
import io.trino.metastore.Column;
import io.trino.metastore.Database;
import io.trino.metastore.HiveColumnStatistics;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HivePartition;
import io.trino.metastore.HivePrincipal;
import io.trino.metastore.HivePrivilegeInfo;
import io.trino.metastore.HiveType;
import io.trino.metastore.Partition;
import io.trino.metastore.PartitionStatistics;
import io.trino.metastore.PartitionWithStatistics;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StatisticsUpdateMode;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.metastore.TableInfo;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.function.LanguageFunction;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.security.RoleGrant;
import io.trino.spi.statistics.ColumnStatisticType;
import io.trino.spi.type.Type;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.catalog.SessionCatalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.RESTSessionCatalog;
import org.apache.iceberg.types.Types;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class PolarisHiveMetastore
        implements HiveMetastore
{
    private final PolarisRestClient polarisClient;
    private final RESTSessionCatalog restSessionCatalog;

    @Inject
    public PolarisHiveMetastore(PolarisRestClient polarisClient, RESTSessionCatalog restSessionCatalog)
    {
        this.polarisClient = requireNonNull(polarisClient, "polarisClient is null");
        this.restSessionCatalog = requireNonNull(restSessionCatalog, "restSessionCatalog is null");
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            // Check if namespace exists by trying to list namespaces and find this one
            List<PolarisNamespace> namespaces = polarisClient.listNamespaces(Optional.empty());
            Optional<PolarisNamespace> namespace = namespaces.stream()
                    .filter(ns -> ns.getName().equals(databaseName))
                    .findFirst();

            if (namespace.isPresent()) {
                return Optional.of(Database.builder()
                        .setDatabaseName(namespace.get().getName())
                        .setParameters(namespace.get().getProperties())
                        .build());
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
        try {
            return polarisClient.listNamespaces(Optional.empty())
                    .stream()
                    .map(PolarisNamespace::getName)
                    .collect(toImmutableList());
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to list databases", e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            // First, try to load as an Iceberg table
            TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);
            SessionCatalog.SessionContext sessionContext = createSessionContext();
            org.apache.iceberg.Table icebergTable = restSessionCatalog.loadTable(sessionContext, tableId);

            // Convert Iceberg table to Hive representation
            return Optional.of(convertIcebergToHiveTable(databaseName, tableName, icebergTable));
        }
        catch (NoSuchTableException e) {
            // Not an Iceberg table, try as a generic table
            try {
                PolarisGenericTable genericTable = polarisClient.loadGenericTable(databaseName, tableName);
                return Optional.of(convertGenericToHiveTable(databaseName, genericTable));
            }
            catch (TableNotFoundException ex) {
                return Optional.empty();
            }
        }
    }

    private SessionCatalog.SessionContext createSessionContext()
    {
        String sessionId = UUID.randomUUID().toString();
        Map<String, String> credentials = ImmutableMap.of();
        Map<String, String> properties = ImmutableMap.of();

        return new SessionCatalog.SessionContext(sessionId, "trino-user", credentials, properties, null);
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
        throw new UnsupportedOperationException("Table statistics updates are not supported by Polaris");
    }

    @Override
    public void updatePartitionStatistics(Table table, StatisticsUpdateMode mode, Map<String, PartitionStatistics> partitionUpdates)
    {
        throw new UnsupportedOperationException("Partition statistics updates are not supported by Polaris");
    }

    public List<String> getAllTables(String databaseName)
    {
        try {
            // Get both Iceberg and generic tables
            List<String> icebergTables = polarisClient.listIcebergTables(databaseName)
                    .stream()
                    .map(PolarisTableIdentifier::getName)
                    .collect(toImmutableList());

            List<String> genericTables = polarisClient.listGenericTables(databaseName)
                    .stream()
                    .map(PolarisTableIdentifier::getName)
                    .collect(toImmutableList());

            return ImmutableList.<String>builder()
                    .addAll(icebergTables)
                    .addAll(genericTables)
                    .build();
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
        try {
            PolarisNamespace namespace = new PolarisNamespace(database.getDatabaseName(), database.getParameters());
            polarisClient.createNamespace(namespace);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create database: " + database.getDatabaseName(), e);
        }
    }

    @Override
    public void dropDatabase(String databaseName, boolean deleteData)
    {
        throw new TrinoException(NOT_SUPPORTED, "Drop database is not supported by Polaris");
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
        try {
            String databaseName = table.getDatabaseName();
            String tableName = table.getTableName();

            // Detect table format based on table parameters
            String tableFormat = detectTableFormat(table);

            switch (tableFormat) {
                case "ICEBERG" -> createIcebergTable(databaseName, tableName, table);
                case "DELTA" -> createGenericTable(databaseName, tableName, table);
                default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported table format: " + tableFormat);
            }
        }
        catch (PolarisAlreadyExistsException e) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + table.getDatabaseName() + "." + table.getTableName(), e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to create table: " + table.getDatabaseName() + "." + table.getTableName(), e);
        }
    }

    private String detectTableFormat(Table table)
    {
        Map<String, String> parameters = table.getParameters();

        // Check for Iceberg table indicators
        if (parameters.containsKey("table_type") && "ICEBERG".equals(parameters.get("table_type"))) {
            return "ICEBERG";
        }
        if (parameters.containsKey("metadata_location")) {
            return "ICEBERG";
        }

        // Check for Delta Lake table indicators
        if (parameters.containsKey("spark.sql.sources.provider") && "DELTA".equals(parameters.get("spark.sql.sources.provider"))) {
            return "DELTA";
        }
        if (parameters.containsKey("spark.sql.sources.provider") && "delta".equalsIgnoreCase(parameters.get("spark.sql.sources.provider"))) {
            return "DELTA";
        }

        // Check for CSV format
        if (parameters.containsKey("format") && "csv".equalsIgnoreCase(parameters.get("format"))) {
            return "CSV";
        }

        // Default to Delta Lake for generic tables
        return "DELTA";
    }

    private void createIcebergTable(String databaseName, String tableName, Table table)
    {
        Schema icebergSchema = convertHiveToIcebergSchema(table.getDataColumns());
        TableIdentifier tableIdentifier = TableIdentifier.of(databaseName, tableName);
        SessionCatalog.SessionContext sessionContext = createSessionContext();
        Map<String, String> properties = table.getParameters();
        String location = table.getStorage().getLocation();

        // Use the RESTSessionCatalog to build and create the table
        Transaction transaction = restSessionCatalog.buildTable(sessionContext, tableIdentifier, icebergSchema)
                .withLocation(location)
                .withProperties(properties)
                .createTransaction();

        transaction.commitTransaction();
    }

    private void createGenericTable(String databaseName, String tableName, Table table)
    {
        // For Delta Lake and other generic tables, use the generic table API
        PolarisGenericTable genericTable = convertHiveToGenericTable(table);
        polarisClient.createGenericTable(databaseName, genericTable);
    }

    private PolarisGenericTable convertHiveToGenericTable(Table table)
    {
        String location = table.getStorage().getLocation();
        Map<String, String> parameters = table.getParameters();

        // Determine format from table properties
        String format = detectTableFormat(table).toLowerCase();

        // Extract comment/description
        String doc = table.getParameters().get("comment");

        // Copy all table properties except internal ones
        ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
        parameters.forEach((key, value) -> {
            if (!key.startsWith("trino.") && !key.equals("comment")) {
                properties.put(key, value);
            }
        });

        return new PolarisGenericTable(
                table.getTableName(),
                format,
                location,
                doc,
                properties.build());
    }

    private Schema convertHiveToIcebergSchema(List<Column> columns)
    {
        AtomicInteger columnId = new AtomicInteger(1);
        List<Types.NestedField> icebergColumns = columns.stream()
                .map(column -> Types.NestedField.optional(
                        columnId.getAndIncrement(),
                        column.getName(),
                        convertHiveTypeToIcebergType(column.getType()),
                        column.getComment().orElse(null)))
                .collect(toImmutableList());
        return new Schema(icebergColumns);
    }

    private org.apache.iceberg.types.Type convertHiveTypeToIcebergType(HiveType hiveType)
    {
        String typeName = hiveType.getHiveTypeName().toString().toLowerCase();
        return switch (typeName) {
            case "boolean" -> Types.BooleanType.get();
            case "tinyint", "smallint", "int" -> Types.IntegerType.get();
            case "bigint" -> Types.LongType.get();
            case "float" -> Types.FloatType.get();
            case "double" -> Types.DoubleType.get();
            case "string" -> Types.StringType.get();
            case "binary" -> Types.BinaryType.get();
            case "date" -> Types.DateType.get();
            case "timestamp" -> Types.TimestampType.withZone();
            default -> Types.StringType.get(); // Default fallback
        };
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            // Try to drop as Iceberg table first
            try {
                SessionCatalog.SessionContext sessionContext = createSessionContext();
                TableIdentifier tableId = TableIdentifier.of(databaseName, tableName);
                if (deleteData) {
                    restSessionCatalog.purgeTable(sessionContext, tableId);
                }
                else {
                    restSessionCatalog.dropTable(sessionContext, tableId);
                }
                return;
            }
            catch (NoSuchTableException ignored) {
                // Fall through to try generic table
            }

            // Try to drop as generic table
            polarisClient.dropGenericTable(databaseName, tableName);
        }
        catch (PolarisNotFoundException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to drop table: " + databaseName + "." + tableName, e);
        }
    }

    @Override
    public void replaceTable(String databaseName, String tableName, Table newTable, PrincipalPrivileges principalPrivileges, Map<String, String> environmentContext)
    {
        throw new TrinoException(NOT_SUPPORTED, "Replace table is not supported by Polaris");
    }

    @Override
    public void renameTable(String databaseName, String tableName, String newDatabaseName, String newTableName)
    {
        try {
            // Try to rename as Iceberg table first
            try {
                SessionCatalog.SessionContext sessionContext = createSessionContext();
                TableIdentifier from = TableIdentifier.of(databaseName, tableName);
                TableIdentifier to = TableIdentifier.of(newDatabaseName, newTableName);
                restSessionCatalog.renameTable(sessionContext, from, to);
                return;
            }
            catch (NoSuchTableException ignored) {
                // Fall through - table might be a generic table
            }

            // Generic table rename is not supported by Polaris API
            throw new TrinoException(NOT_SUPPORTED, "Rename table is not supported for Delta Lake tables in Polaris");
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_METASTORE_ERROR, "Failed to rename table: " + databaseName + "." + tableName, e);
        }
    }

    @Override
    public void commentTable(String databaseName, String tableName, Optional<String> comment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Table comments are not supported by Polaris");
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

    // Partition operations - handled by format-specific connectors
    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return Optional.empty();
    }

    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
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
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    @Override
    public void alterPartition(String databaseName, String tableName, PartitionWithStatistics partition)
    {
        throw new UnsupportedOperationException("Partition management is handled by format-specific connectors");
    }

    // Role and privilege operations - not supported by Polaris
    @Override
    public void createRole(String role, String grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public void dropRole(String role)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public Set<String> listRoles()
    {
        return ImmutableSet.of();
    }

    @Override
    public void grantRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    @Override
    public void revokeRoles(Set<String> roles, Set<HivePrincipal> grantees, boolean adminOption, HivePrincipal grantor)
    {
        throw new UnsupportedOperationException("Role management is not supported by Polaris");
    }

    public Set<RoleGrant> listGrantedPrincipals(String role)
    {
        return ImmutableSet.of();
    }

    @Override
    public Set<RoleGrant> listRoleGrants(HivePrincipal principal)
    {
        return ImmutableSet.of();
    }

    // Transaction operations - not supported by Polaris
    @Override
    public Optional<String> getConfigValue(String name)
    {
        return Optional.empty();
    }

    @Override
    public long openTransaction(AcidTransactionOwner transactionOwner)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void commitTransaction(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void abortTransaction(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void sendTransactionHeartbeat(long transactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    @Override
    public void acquireSharedReadLock(AcidTransactionOwner transactionOwner, String queryId, long transactionId, List<SchemaTableName> fullTables, List<HivePartition> partitions)
    {
        // No-op for Polaris
    }

    @Override
    public String getValidWriteIds(List<SchemaTableName> tables, long currentTransactionId)
    {
        throw new UnsupportedOperationException("ACID transactions are not supported by Polaris");
    }

    // Function operations - not supported by Polaris
    @Override
    public boolean functionExists(String databaseName, String functionName, String signatureToken)
    {
        return false;
    }

    @Override
    public Collection<LanguageFunction> getFunctions(String databaseName, String functionName)
    {
        return ImmutableList.of();
    }

    @Override
    public Collection<LanguageFunction> getAllFunctions(String databaseName)
    {
        return ImmutableList.of();
    }

    @Override
    public void createFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    public void alterFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    @Override
    public void dropFunction(String databaseName, String functionName, String signatureToken)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    @Override
    public void replaceFunction(String databaseName, String functionName, LanguageFunction function)
    {
        throw new UnsupportedOperationException("Function management is not supported by Polaris");
    }

    // Column statistics operations - not supported yet
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

    // Schema modification operations - not supported
    @Override
    public void addColumn(String databaseName, String tableName, String columnName, HiveType columnType, String columnComment)
    {
        throw new TrinoException(NOT_SUPPORTED, "Add column is not supported by Polaris");
    }

    @Override
    public void renameColumn(String databaseName, String tableName, String oldColumnName, String newColumnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Rename column is not supported by Polaris");
    }

    @Override
    public void dropColumn(String databaseName, String tableName, String columnName)
    {
        throw new TrinoException(NOT_SUPPORTED, "Drop column is not supported by Polaris");
    }

    // Privilege operations - not supported
    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("Table privileges are not supported by Polaris");
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String tableOwner, HivePrincipal grantee, HivePrincipal grantor, Set<HivePrivilegeInfo.HivePrivilege> privileges, boolean grantOption)
    {
        throw new UnsupportedOperationException("Table privileges are not supported by Polaris");
    }

    @Override
    public Set<HivePrivilegeInfo> listTablePrivileges(String databaseName, String tableName, Optional<String> tableOwner, Optional<HivePrincipal> principal)
    {
        return ImmutableSet.of();
    }

    // Helper methods for converting Polaris tables to Hive tables
    private Table convertIcebergToHiveTable(String databaseName, String tableName, org.apache.iceberg.Table icebergTable)
    {
        return Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(tableName)
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(ImmutableList.of()) // Schema will be handled by Iceberg connector
                .setParameters(ImmutableMap.<String, String>builder()
                        .putAll(icebergTable.properties())
                        .put("table_type", "ICEBERG")
                        .put("metadata_location", icebergTable.location())
                        .build())
                .withStorage(storage -> storage
                        .setLocation(icebergTable.location())
                        .setStorageFormat(StorageFormat.create(
                                "org.apache.hadoop.mapred.FileInputFormat",
                                "org.apache.hadoop.mapred.FileOutputFormat",
                                "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
                        .setSerdeParameters(ImmutableMap.of("path", icebergTable.location())))
                .build();
    }

    private Table convertGenericToHiveTable(String databaseName, PolarisGenericTable genericTable)
    {
        // Since generic tables don't have schema information in Polaris,
        // we'll create a table with minimal column information.
        // The actual schema will be determined by the engine (Delta Lake, CSV reader, etc.)
        List<Column> columns = ImmutableList.of();

        // Determine storage format based on generic table format
        StorageFormat storageFormat = switch (genericTable.getFormat().toLowerCase()) {
            case "delta" -> StorageFormat.create(
                    "org.apache.hadoop.mapred.FileInputFormat",
                    "org.apache.hadoop.mapred.FileOutputFormat",
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
            case "csv" -> StorageFormat.create(
                    "org.apache.hadoop.mapred.TextInputFormat",
                    "org.apache.hadoop.mapred.HiveIgnoreKeyTextOutputFormat",
                    "org.apache.hadoop.hive.serde2.OpenCSVSerde");
            default -> StorageFormat.create(
                    "org.apache.hadoop.mapred.FileInputFormat",
                    "org.apache.hadoop.mapred.FileOutputFormat",
                    "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe");
        };

        ImmutableMap.Builder<String, String> parameters = ImmutableMap.builder();
        parameters.putAll(genericTable.getProperties());

        // Add format information
        if ("delta".equalsIgnoreCase(genericTable.getFormat())) {
            parameters.put("spark.sql.sources.provider", "DELTA");
        }
        parameters.put("format", genericTable.getFormat());

        // Add comment if present
        genericTable.getDoc().ifPresent(doc -> parameters.put("comment", doc));

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(databaseName)
                .setTableName(genericTable.getName())
                .setTableType("EXTERNAL_TABLE")
                .setDataColumns(columns)
                .setParameters(parameters.build());

        // Set storage information
        genericTable.getBaseLocation().ifPresent(location ->
                tableBuilder.withStorage(storage -> storage
                    .setLocation(location)
                    .setStorageFormat(storageFormat)
                    .setSerdeParameters(ImmutableMap.of("path", location))));

        return tableBuilder.build();
    }
}
