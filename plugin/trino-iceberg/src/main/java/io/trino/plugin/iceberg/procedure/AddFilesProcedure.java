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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.airlift.log.Logger;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Partition;
import io.trino.metastore.Storage;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.RawHiveMetastoreFactory;
import io.trino.plugin.iceberg.ColumnIdentity;
import io.trino.plugin.iceberg.IcebergConfig;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.catalog.TrinoCatalogFactory;
import io.trino.plugin.iceberg.procedure.HiveMigrations.RecursiveDirectory;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveMetadata.TRANSACTIONAL;
import static io.trino.plugin.hive.HiveMetadata.extractHiveStorageFormat;
import static io.trino.plugin.hive.HiveStorageFormat.AVRO;
import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.util.HiveTypeUtil.getTypeSignature;
import static io.trino.plugin.hive.util.HiveUtil.isDeltaLakeTable;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.plugin.hive.util.HiveUtil.isIcebergTable;
import static io.trino.plugin.iceberg.ColumnIdentity.createColumnIdentity;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isMergeManifestsOnWrite;
import static io.trino.plugin.iceberg.TypeConverter.toIcebergType;
import static io.trino.plugin.iceberg.procedure.HiveMigrations.buildDataFile;
import static io.trino.plugin.iceberg.procedure.HiveMigrations.loadMetrics;
import static io.trino.spi.StandardErrorCode.COLUMN_NOT_FOUND;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.StandardErrorCode.PERMISSION_DENIED;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Boolean.parseBoolean;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;

public class AddFilesProcedure
        implements Provider<Procedure>
{
    private static final Logger log = Logger.get(AddFilesProcedure.class);

    private final TrinoCatalogFactory catalogFactory;
    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;
    private final TypeManager typeManager;
    private final boolean addFilesProcedureEnabled;

    private enum SourceType
    {
        TABLE,
        LOCATION,
        /**/
    }

    private static final MethodHandle ADD_FILES;

    static {
        try {
            ADD_FILES = lookup().unreflect(AddFilesProcedure.class.getMethod(
                    "addFiles",
                    ConnectorSession.class,
                    ConnectorAccessControl.class,
                    String.class,
                    String.class,
                    String.class,
                    String.class,
                    List.class,
                    String.class,
                    String.class,
                    String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    @Inject
    public AddFilesProcedure(
            TrinoCatalogFactory catalogFactory,
            @RawHiveMetastoreFactory HiveMetastoreFactory metastoreFactory,
            TrinoFileSystemFactory fileSystemFactory,
            TypeManager typeManager,
            IcebergConfig icebergConfig)
    {
        this.catalogFactory = requireNonNull(catalogFactory, "catalogFactory is null");
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.addFilesProcedureEnabled = icebergConfig.isAddFilesProcedureEnabled();
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "add_files",
                ImmutableList.<Procedure.Argument>builder()
                        .add(new Procedure.Argument("TARGET_SCHEMA_NAME", VARCHAR))
                        .add(new Procedure.Argument("TARGET_TABLE_NAME", VARCHAR))
                        .add(new Procedure.Argument("SOURCE_SCHEMA_NAME", VARCHAR, false, null))
                        .add(new Procedure.Argument("SOURCE_TABLE_NAME", VARCHAR, false, null))
                        .add(new Procedure.Argument("PARTITION_FILTER", new ArrayType(VARCHAR), false, null))
                        .add(new Procedure.Argument("LOCATION", VARCHAR, false, null))
                        .add(new Procedure.Argument("FORMAT", VARCHAR, false, null))
                        .add(new Procedure.Argument("RECURSIVE_DIRECTORY", VARCHAR, false, utf8Slice("fail")))
                        .build(),
                ADD_FILES.bindTo(this));
    }

    public void addFiles(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String targetSchemaName,
            String targetTableName,
            @Nullable String sourceSchemaName,
            @Nullable String sourceTableName,
            @Nullable List<String> partitionFilter,
            @Nullable String location,
            @Nullable String format,
            String recursiveDirectory)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doAddFiles(
                    session,
                    accessControl,
                    targetSchemaName,
                    targetTableName,
                    sourceSchemaName,
                    sourceTableName,
                    partitionFilter,
                    location,
                    format,
                    recursiveDirectory);
        }
    }

    public void doAddFiles(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String targetSchemaName,
            String targetTableName,
            @Nullable String sourceSchemaName,
            @Nullable String sourceTableName,
            @Nullable List<String> partitionFilter,
            @Nullable String location,
            @Nullable String format,
            String recursiveDirectory)
    {
        if (!addFilesProcedureEnabled) {
            throw new TrinoException(PERMISSION_DENIED, "add_files procedure is disabled");
        }

        TrinoCatalog catalog = catalogFactory.create(session.getIdentity());
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));
        RecursiveDirectory recursive = Enums.getIfPresent(RecursiveDirectory.class, recursiveDirectory.toUpperCase(ENGLISH)).toJavaUtil()
                .orElseThrow(() -> new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Invalid recursive_directory: " + recursiveDirectory));

        SchemaTableName targetName = new SchemaTableName(targetSchemaName, targetTableName);
        io.trino.metastore.Table targetTable = metastore.getTable(targetSchemaName, targetTableName).orElseThrow(() -> new TableNotFoundException(targetName));
        if (!isIcebergTable(targetTable)) {
            throw new TrinoException(NOT_SUPPORTED, "The target table must be Iceberg table");
        }
        accessControl.checkCanInsertIntoTable(null, targetName);

        Table table = catalog.loadTable(session, targetName);
        PartitionSpec partitionSpec = table.spec();
        Schema schema = table.schema();
        NameMapping nameMapping = MappingUtil.create(schema);
        Set<Integer> requiredFields = schema.columns().stream()
                .filter(Types.NestedField::isRequired)
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        HiveStorageFormat storageFormat;
        String sourceLocation;
        io.trino.metastore.Table sourceTable = null;
        SourceType sourceType;
        if (sourceSchemaName != null && sourceTableName != null) {
            checkProcedureArgument(location == null, "LOCATION argument must be null");
            checkProcedureArgument(format == null, "FORMAT argument must be null");

            SchemaTableName sourceName = new SchemaTableName(sourceSchemaName, sourceTableName);
            sourceTable = metastore.getTable(sourceSchemaName, sourceTableName).orElseThrow(() -> new TableNotFoundException(sourceName));

            accessControl.checkCanSelectFromColumns(null, sourceName, Stream.concat(sourceTable.getDataColumns().stream(), sourceTable.getPartitionColumns().stream())
                    .map(Column::getName)
                    .collect(toImmutableSet()));

            checkProcedureArgument(
                    table.schemas().size() == sourceTable.getDataColumns().size(),
                    "Data column count mismatch: %d vs %d", table.schemas().size(), sourceTable.getDataColumns().size());
            for (Column sourceColumn : Stream.concat(sourceTable.getDataColumns().stream(), sourceTable.getPartitionColumns().stream()).toList()) {
                Types.NestedField targetColumn = schema.caseInsensitiveFindField(sourceColumn.getName());
                if (targetColumn == null) {
                    throw new TrinoException(COLUMN_NOT_FOUND, "Column '%s' does not exist".formatted(sourceColumn.getName()));
                }
                ColumnIdentity columnIdentity = createColumnIdentity(targetColumn);
                org.apache.iceberg.types.Type sourceColumnType = toIcebergType(typeManager.getType(getTypeSignature(sourceColumn.getType(), DEFAULT_PRECISION)), columnIdentity);
                if (!targetColumn.type().equals(sourceColumnType)) {
                    throw new TrinoException(TYPE_MISMATCH, "Expected target '%s' type, but got source '%s' type".formatted(targetColumn.type(), sourceColumnType));
                }
            }

            String transactionalProperty = sourceTable.getParameters().get(TRANSACTIONAL);
            if (parseBoolean(transactionalProperty)) {
                throw new TrinoException(NOT_SUPPORTED, "Adding files from transactional tables is unsupported");
            }
            if (!"MANAGED_TABLE".equalsIgnoreCase(sourceTable.getTableType()) && !"EXTERNAL_TABLE".equalsIgnoreCase(sourceTable.getTableType())) {
                throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from %s table type".formatted(sourceTable.getTableType()));
            }
            if (isDeltaLakeTable(sourceTable)) {
                throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from Delta Lake tables");
            }
            if (isHudiTable(sourceTable)) {
                throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from Hudi tables");
            }
            if (isIcebergTable(sourceTable)) {
                throw new TrinoException(NOT_SUPPORTED, "The procedure doesn't support adding files from Iceberg tables");
            }
            if (sourceTable.getPartitionColumns().isEmpty() && partitionFilter != null && !partitionFilter.isEmpty()) {
                throw new TrinoException(NOT_SUPPORTED, "Partition filter is not supported for non-partitioned tables");
            }
            storageFormat = extractHiveStorageFormat(sourceTable.getStorage().getStorageFormat());
            sourceLocation = sourceTable.getStorage().getLocation();
            sourceType = SourceType.TABLE;
        }
        else if (location != null && format != null) {
            checkProcedureArgument(sourceSchemaName == null, "SOURCE_SCHEMA_NAME argument must be null");
            checkProcedureArgument(sourceTableName == null, "SOURCE_TABLE_NAME argument must be null");
            checkProcedureArgument(partitionFilter == null, "PARTITION_FILTER argument must be null");

            sourceLocation = location;
            storageFormat = HiveStorageFormat.valueOf(format.toUpperCase(ENGLISH));
            checkProcedureArgument(storageFormat == ORC || storageFormat == PARQUET || storageFormat == AVRO, "The procedure does not support storage format: %s", storageFormat);
            sourceType = SourceType.LOCATION;
        }
        else {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Either source schema and table names, or location and format must be provided");
        }

        try {
            TrinoFileSystem fileSystem = fileSystemFactory.create(session);
            ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
            if (partitionSpec.isUnpartitioned()) {
                log.debug("Building data files from %s", sourceLocation);
                dataFilesBuilder.addAll(buildDataFiles(fileSystem, sourceType, recursive, storageFormat, sourceLocation, partitionSpec, Optional.empty(), schema, requiredFields));
            }
            else {
                checkProcedureArgument(location == null, "The procedure does not support partitioned tables with location argument");

                Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(sourceTable, firstNonNull(partitionFilter, ImmutableList.of()));
                int fileCount = 1;
                for (Map.Entry<String, Optional<Partition>> partition : partitions.entrySet()) {
                    Storage storage = partition.getValue().orElseThrow(() -> new IllegalArgumentException("Invalid partition: " + partition.getKey())).getStorage();
                    log.debug("Building data files from '%s' for partition %d of %d", storage.getLocation(), fileCount++, partitions.size());
                    HiveStorageFormat partitionStorageFormat = extractHiveStorageFormat(storage.getStorageFormat());
                    StructLike partitionData = DataFiles.data(partitionSpec, partition.getKey());
                    dataFilesBuilder.addAll(buildDataFiles(fileSystem, sourceType, recursive, partitionStorageFormat, storage.getLocation(), partitionSpec, Optional.of(partitionData), schema, requiredFields));
                }
            }

            log.debug("Start new transaction");
            Transaction transaction = table.newTransaction();
            if (!table.properties().containsKey(DEFAULT_NAME_MAPPING)) {
                log.debug("Update default name mapping property");
                transaction.updateProperties()
                        .set(DEFAULT_NAME_MAPPING, toJson(nameMapping))
                        .commit();
            }
            List<DataFile> dataFiles = dataFilesBuilder.build();
            log.debug("Append data %d data files", dataFiles.size());
            AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
            dataFiles.forEach(appendFiles::appendFile);
            appendFiles.commit();
            transaction.commitTransaction();
            log.debug("Successfully added files to %s table", targetName);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add files: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    private static List<DataFile> buildDataFiles(
            TrinoFileSystem fileSystem,
            SourceType sourceType,
            RecursiveDirectory recursive,
            HiveStorageFormat format,
            String location,
            PartitionSpec partitionSpec,
            Optional<StructLike> partition,
            Schema schema,
            Set<Integer> requiredFields)
            throws IOException
    {
        List<DataFile> dataFiles = switch (sourceType) {
            case TABLE -> HiveMigrations.buildDataFiles(fileSystem, recursive, format, location, partitionSpec, partition, schema);
            case LOCATION -> buildDataFilesFromLocation(fileSystem, recursive, format, location, partitionSpec, partition, schema);
        };

        if (!requiredFields.isEmpty()) {
            for (DataFile dataFile : dataFiles) {
                Map<Integer, Long> nullValueCounts = dataFile.nullValueCounts();
                for (Integer field : requiredFields) {
                    if (nullValueCounts.get(field) > 0) {
                        throw new TrinoException(CONSTRAINT_VIOLATION, "NULL value not allowed for NOT NULL column: " + schema.findField(field).name());
                    }
                }
            }
        }

        return dataFiles;
    }

    private static List<DataFile> buildDataFilesFromLocation(
            TrinoFileSystem fileSystem,
            RecursiveDirectory recursive,
            HiveStorageFormat format,
            String location,
            PartitionSpec partitionSpec,
            Optional<StructLike> partition,
            Schema schema)
            throws IOException
    {
        if (fileSystem.directoryExists(Location.of(location)).orElse(false)) {
            return HiveMigrations.buildDataFiles(fileSystem, recursive, format, location, partitionSpec, partition, schema);
        }

        TrinoInputFile file = fileSystem.newInputFile(Location.of(location));
        if (file.exists()) {
            Metrics metrics = loadMetrics(file, format, schema);
            return ImmutableList.of(buildDataFile(file.location().toString(), file.length(), partition, partitionSpec, format.name(), metrics));
        }

        throw new TrinoException(NOT_FOUND, "Location not found: " + location);
    }
}
