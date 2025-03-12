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
package io.trino.plugin.iceberg.procedure;

import com.google.common.base.Enums;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.RawHiveMetastoreFactory;
import io.trino.metastore.Storage;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.security.UsingSystemSecurity;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.procedure.MigrationUtils.RecursiveDirectory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.SmallintType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TinyintType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.concat;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveMetadata.extractHiveStorageFormat;
import static io.trino.plugin.hive.HiveTimestampPrecision.MILLISECONDS;
import static io.trino.plugin.hive.metastore.MetastoreUtil.buildInitialPrivilegeSet;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergTypeForNewColumn;
import static io.trino.plugin.iceberg.procedure.MigrationUtils.buildDataFiles;
import static io.trino.spi.StandardErrorCode.DUPLICATE_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.BaseMetastoreTableOperations.ICEBERG_TABLE_TYPE_VALUE;
import static org.apache.iceberg.BaseMetastoreTableOperations.METADATA_LOCATION_PROP;
import static org.apache.iceberg.BaseMetastoreTableOperations.TABLE_TYPE_PROP;
import static org.apache.iceberg.SortOrder.unsorted;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.TableProperties.FORMAT_VERSION;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;

public class MigrateProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(MigrateProcedure.class);

    public static final String PROVIDER_PROPERTY_KEY = "provider";
    public static final String PROVIDER_PROPERTY_VALUE = "iceberg";

    private final TrinoCatalogFactory catalogFactory;
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final int formatVersion;
    private final boolean isUsingSystemSecurity;

    private static final MethodHandle MIGRATE;

    static {
        try {
            MIGRATE = lookup().unreflect(MigrateProcedure.class.getMethod("migrate", ConnectorSession.class, String.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Inject
    public MigrateProcedure(
            TrinoCatalogFactory catalogFactory,
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergConfig icebergConfig,
            @UsingSystemSecurity boolean usingSystemSecurity)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.formatVersion = icebergConfig.getFormatVersion();
        this.isUsingSystemSecurity = usingSystemSecurity;
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "migrate",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("RECURSIVE_DIRECTORY", VARCHAR, false, utf8Slice("fail"))),
                MIGRATE.bindTo(this));
    }

    public void migrate(ConnectorSession session, String schemaName, String tableName, String recursiveDirectory)
    {
        // this line guarantees that classLoader that we stored in the field will be used inside try/catch
        // as we captured reference to PluginClassLoader during initialization of this class
        // we can use it now to correctly execute the procedure
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doMigrate(session, schemaName, tableName, recursiveDirectory);
        }
    }

    public void doMigrate(ConnectorSession session, String schemaName, String tableName, String recursiveDirectory)
    {
        SchemaTableName sourceTableName = new SchemaTableName(schemaName, tableName);
        TrinoCatalog catalog = catalogFactory.create(session.getIdentity());
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));
        RecursiveDirectory recursive = Enums.getIfPresent(RecursiveDirectory.class, recursiveDirectory.toUpperCase(ENGLISH)).toJavaUtil()
                .orElseThrow(() -> new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid recursive_directory: " + recursiveDirectory));

        io.trino.metastore.Table hiveTable = metastore.getTable(schemaName, tableName).orElseThrow(() -> new TableNotFoundException(sourceTableName));
        String transactionalProperty = hiveTable.getParameters().get(TRANSACTIONAL);
        if (parseBoolean(transactionalProperty)) {
            throw new TrinoException(NOT_SUPPORTED, "Migrating transactional tables is unsupported");
        }
        if (!"MANAGED_TABLE".equalsIgnoreCase(hiveTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(hiveTable.getTableType())) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support migrating %s table type".formatted(hiveTable.getTableType()));
        }
        if (isDeltaLakeTable(hiveTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support migrating Delta Lake tables");
        }
        if (isHudiTable(hiveTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support migrating Hudi tables");
        }
        if (isIcebergTable(hiveTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The table is already an Iceberg table");
        }

        HiveStorageFormat storageFormat = extractHiveStorageFormat(hiveTable.getStorage().getStorageFormat());
        Schema schema = toIcebergSchema(concat(hiveTable.getDataColumns().stream(), hiveTable.getPartitionColumns().stream()).toList(), toIcebergFileFormat(storageFormat));
        NameMapping nameMapping = MappingUtil.create(schema);
        String location = hiveTable.getStorage().getLocation();

        Map<String, String> properties = icebergTableProperties(location, hiveTable.getParameters(), nameMapping, toIcebergFileFormat(storageFormat));
        PartitionSpec partitionSpec = parsePartitionFields(schema, getPartitionColumnNames(hiveTable));
        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
            if (hiveTable.getPartitionColumns().isEmpty()) {
                log.debug("Building data files from %s", location);
                dataFilesBuilder.addAll(buildDataFiles(fileSystem, recursive, storageFormat, location, partitionSpec, Optional.empty(), schema));
            }
            else {
                Map<String, Optional<Partition>> partitions = listAllPartitions(metastore, hiveTable);
                int fileCount = 1;
                for (Map.Entry<String, Optional<Partition>> partition : partitions.entrySet()) {
                    Storage storage = partition.getValue().orElseThrow().getStorage();
                    log.debug("Building data files from '%s' for partition %d of %d", storage.getLocation(), fileCount++, partitions.size());
                    HiveStorageFormat partitionStorageFormat = extractHiveStorageFormat(storage.getStorageFormat());
                    StructLike partitionData = DataFiles.data(partitionSpec, partition.getKey());
                    dataFilesBuilder.addAll(buildDataFiles(fileSystem, recursive, partitionStorageFormat, storage.getLocation(), partitionSpec, Optional.of(partitionData), schema));
                }
            }

            log.debug("Start new transaction");
            Transaction transaction = catalog.newCreateTableTransaction(
                    session,
                    sourceTableName,
                    schema,
                    parsePartitionFields(schema, toPartitionFields(hiveTable)),
                    unsorted(),
                    Optional.of(location),
                    properties);

            List<DataFile> dataFiles = dataFilesBuilder.build();
            log.debug("Append data %d data files", dataFiles.size());
            Table table = transaction.table();
            AppendFiles append = table.newAppend();
            dataFiles.forEach(append::appendFile);
            append.commit();

            log.debug("Set preparatory table properties in a metastore for migrations");
            PrincipalPrivileges principalPrivileges = isUsingSystemSecurity ? NO_PRIVILEGES : buildInitialPrivilegeSet(session.getUser());
            io.trino.metastore.Table newTable = io.trino.metastore.Table.builder(hiveTable)
                    .setParameter(METADATA_LOCATION_PROP, location)
                    .setParameter(TABLE_TYPE_PROP, ICEBERG_TABLE_TYPE_VALUE.toUpperCase(ENGLISH))
                    .build();
            metastore.replaceTable(schemaName, tableName, newTable, principalPrivileges);

            transaction.commitTransaction();
            log.debug("Successfully migrated %s table to Iceberg format", sourceTableName);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to migrate table", e);
        }
    }

    private Map<String, String> icebergTableProperties(String location, Map<String, String> hiveTableProperties, NameMapping nameMapping, IcebergFileFormat fileFormat)
    {
        Map<String, String> icebergTableProperties = new HashMap<>();

        // Copy all Hive table properties so that we can undo the migration easily. This is same as Spark implementation.
        icebergTableProperties.putAll(hiveTableProperties);
        icebergTableProperties.remove("path");
        icebergTableProperties.remove("transient_lastDdlTime");
        icebergTableProperties.remove("serialization.format");

        icebergTableProperties.put("migrated", "true");
        icebergTableProperties.putIfAbsent("location", location);
        icebergTableProperties.put(PROVIDER_PROPERTY_KEY, PROVIDER_PROPERTY_VALUE);
        icebergTableProperties.put(METADATA_LOCATION_PROP, location);
        icebergTableProperties.put(DEFAULT_NAME_MAPPING, toJson(nameMapping));
        icebergTableProperties.put(DEFAULT_FILE_FORMAT, fileFormat.name());
        icebergTableProperties.put(FORMAT_VERSION, String.valueOf(formatVersion));

        return ImmutableMap.copyOf(icebergTableProperties);
    }

    private Schema toIcebergSchema(List<Column> columns, IcebergFileFormat storageFormat)
    {
        AtomicInteger nextFieldId = new AtomicInteger(1);
        List<Types.NestedField> icebergColumns = new ArrayList<>();
        for (Column column : columns) {
            int index = icebergColumns.size();
            org.apache.iceberg.types.Type type = toIcebergType(typeManager.getType(getTypeSignature(column.getType(), MILLISECONDS)), nextFieldId, storageFormat);
            Types.NestedField field = Types.NestedField.optional(index, column.getName(), type, column.getComment().orElse(null));
            icebergColumns.add(field);
        }

        org.apache.iceberg.types.Type icebergSchema = Types.StructType.of(icebergColumns);
        // Assign column id start from 1
        icebergSchema = TypeUtil.assignFreshIds(icebergSchema, new AtomicInteger(1)::getAndIncrement);
        return new Schema(icebergSchema.asStructType().fields());
    }

    private static org.apache.iceberg.types.Type toIcebergType(Type type, AtomicInteger nextFieldId, IcebergFileFormat storageFormat)
    {
        return switch (type) {
            case TinyintType _, SmallintType _ -> Types.IntegerType.get();
            case TimestampType _ -> switch (storageFormat) {
                case ORC -> Types.TimestampType.withoutZone();
                case PARQUET -> Types.TimestampType.withZone();
                case AVRO ->  // TODO https://github.com/trinodb/trino/issues/20481
                        throw new TrinoException(NOT_SUPPORTED, "Migrating timestamp type with Avro format is not supported.");
            };
            case RowType rowType -> fromRow(rowType, nextFieldId, storageFormat);
            case ArrayType arrayType -> fromArray(arrayType, nextFieldId, storageFormat);
            case MapType mapType -> fromMap(mapType, nextFieldId, storageFormat);
            default -> toIcebergTypeForNewColumn(type, nextFieldId);
        };
    }

    private static org.apache.iceberg.types.Type fromRow(RowType type, AtomicInteger nextFieldId, IcebergFileFormat storageFormat)
    {
        Set<String> fieldNames = new HashSet<>();
        List<Types.NestedField> fields = new ArrayList<>();
        for (int i = 0; i < type.getFields().size(); i++) {
            int id = nextFieldId.getAndIncrement();
            RowType.Field field = type.getFields().get(i);
            String name = field.getName().orElseThrow(() -> new TrinoException(NOT_SUPPORTED, "Row type field does not have a name: " + type.getDisplayName()));
            if (!fieldNames.add(name.toLowerCase(ENGLISH))) {
                throw new TrinoException(DUPLICATE_COLUMN_NAME, "Field name '%s' specified more than once".formatted(name.toLowerCase(ENGLISH)));
            }
            org.apache.iceberg.types.Type icebergTypeInternal = toIcebergType(field.getType(), nextFieldId, storageFormat);
            fields.add(Types.NestedField.optional(id, name, icebergTypeInternal));
        }
        return Types.StructType.of(fields);
    }

    private static org.apache.iceberg.types.Type fromArray(ArrayType type, AtomicInteger nextFieldId, IcebergFileFormat storageFormat)
    {
        int id = nextFieldId.getAndIncrement();
        return Types.ListType.ofOptional(id, toIcebergType(type.getElementType(), nextFieldId, storageFormat));
    }

    private static org.apache.iceberg.types.Type fromMap(MapType type, AtomicInteger nextFieldId, IcebergFileFormat storageFormat)
    {
        int keyId = nextFieldId.getAndIncrement();
        int valueId = nextFieldId.getAndIncrement();
        return Types.MapType.ofOptional(
                keyId,
                valueId,
                toIcebergType(type.getKeyType(), nextFieldId, storageFormat),
                toIcebergType(type.getValueType(), nextFieldId, storageFormat));
    }

    public Map<String, Optional<Partition>> listAllPartitions(HiveMetastore metastore, io.trino.metastore.Table table)
    {
        List<String> partitionNames = table.getPartitionColumns().stream().map(Column::getName).collect(toImmutableList());
        Optional<List<String>> partitions = metastore.getPartitionNamesByFilter(table.getDatabaseName(), table.getTableName(), partitionNames, TupleDomain.all());
        if (partitions.isEmpty()) {
            return ImmutableMap.of();
        }
        return metastore.getPartitionsByNames(table, partitions.get());
    }

    private static IcebergFileFormat toIcebergFileFormat(HiveStorageFormat storageFormat)
    {
        return switch (storageFormat) {
            case ORC -> IcebergFileFormat.ORC;
            case PARQUET -> IcebergFileFormat.PARQUET;
            case AVRO -> IcebergFileFormat.AVRO;
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported storage format: " + storageFormat);
        };
    }

    private static List<String> toPartitionFields(io.trino.metastore.Table table)
    {
        ImmutableList.Builder<String> fields = ImmutableList.builder();
        fields.addAll(getPartitionColumnNames(table));
        return fields.build();
    }

    private static List<String> getPartitionColumnNames(io.trino.metastore.Table table)
    {
        return table.getPartitionColumns().stream()
                .map(Column::getName)
                .collect(toImmutableList());
    }
}
