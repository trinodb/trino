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

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInputFile;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Partition;
import io.trino.metastore.Storage;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.parquet.TrinoParquetDataSource;
import io.trino.plugin.iceberg.catalog.TrinoCatalog;
import io.trino.plugin.iceberg.fileio.ForwardingInputFile;
import io.trino.plugin.iceberg.util.OrcMetrics;
import io.trino.plugin.iceberg.util.ParquetUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Metrics;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.Transaction;
import org.apache.iceberg.avro.Avro;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.mapping.MappingUtil;
import org.apache.iceberg.mapping.NameMapping;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.HiveMetadata.extractHiveStorageFormat;
import static io.trino.plugin.iceberg.IcebergErrorCode.ICEBERG_COMMIT_ERROR;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isMergeManifestsOnWrite;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.StandardErrorCode.CONSTRAINT_VIOLATION;
import static io.trino.spi.StandardErrorCode.NOT_FOUND;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static org.apache.iceberg.TableProperties.DEFAULT_NAME_MAPPING;
import static org.apache.iceberg.mapping.NameMappingParser.toJson;

public final class MigrationUtils
{
    private static final Logger log = Logger.get(MigrationUtils.class);
    private static final Joiner.MapJoiner PARTITION_JOINER = Joiner.on("/").withKeyValueSeparator("=");

    private static final MetricsConfig METRICS_CONFIG = MetricsConfig.getDefault();

    public enum RecursiveDirectory
    {
        TRUE,
        FALSE,
        FAIL,
        /**/
    }

    private MigrationUtils() {}

    public static List<DataFile> buildDataFiles(
            TrinoFileSystem fileSystem,
            RecursiveDirectory recursive,
            HiveStorageFormat format,
            String location,
            PartitionSpec partitionSpec,
            Optional<StructLike> partition,
            Schema schema)
            throws IOException
    {
        // TODO: Introduce parallelism
        FileIterator files = fileSystem.listFiles(Location.of(location));
        ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
        while (files.hasNext()) {
            FileEntry file = files.next();
            String fileLocation = file.location().toString();
            String relativePath = fileLocation.substring(location.length());
            if (relativePath.contains("/_") || relativePath.contains("/.")) {
                continue;
            }
            if (recursive == RecursiveDirectory.FALSE && isRecursive(location, fileLocation)) {
                continue;
            }
            if (recursive == RecursiveDirectory.FAIL && isRecursive(location, fileLocation)) {
                throw new TrinoException(NOT_SUPPORTED, "Recursive directory must not exist when recursive_directory argument is 'fail': " + file.location());
            }

            Metrics metrics = loadMetrics(fileSystem.newInputFile(file.location(), file.length()), format, schema);
            DataFile dataFile = buildDataFile(fileLocation, file.length(), partition, partitionSpec, format.name(), metrics);
            dataFilesBuilder.add(dataFile);
        }
        List<DataFile> dataFiles = dataFilesBuilder.build();
        log.debug("Found %d files in '%s'", dataFiles.size(), location);
        return dataFiles;
    }

    private static boolean isRecursive(String baseLocation, String location)
    {
        verify(location.startsWith(baseLocation), "%s should start with %s", location, baseLocation);
        String suffix = location.substring(baseLocation.length() + 1).replaceFirst("^/+", "");
        return suffix.contains("/");
    }

    public static Metrics loadMetrics(TrinoInputFile file, HiveStorageFormat storageFormat, Schema schema)
    {
        return switch (storageFormat) {
            case ORC -> OrcMetrics.fileMetrics(file, METRICS_CONFIG, schema);
            case PARQUET -> parquetMetrics(file, METRICS_CONFIG, MappingUtil.create(schema));
            case AVRO -> new Metrics(Avro.rowCount(new ForwardingInputFile(file)), null, null, null, null);
            default -> throw new TrinoException(NOT_SUPPORTED, "Unsupported storage format: " + storageFormat);
        };
    }

    private static Metrics parquetMetrics(TrinoInputFile file, MetricsConfig metricsConfig, NameMapping nameMapping)
    {
        try (ParquetDataSource dataSource = new TrinoParquetDataSource(file, new ParquetReaderOptions(), new FileFormatDataSourceStats())) {
            ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
            return ParquetUtil.footerMetrics(metadata, Stream.empty(), metricsConfig, nameMapping);
        }
        catch (IOException e) {
            throw new UncheckedIOException("Failed to read file footer: " + file.location(), e);
        }
    }

    public static void addFiles(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            TrinoCatalog catalog,
            SchemaTableName targetName,
            String location,
            HiveStorageFormat format,
            RecursiveDirectory recursiveDirectory)
    {
        Table table = catalog.loadTable(session, targetName);
        PartitionSpec partitionSpec = table.spec();

        checkProcedureArgument(partitionSpec.isUnpartitioned(), "The procedure does not support partitioned tables");

        try {
            List<DataFile> dataFiles = buildDataFilesFromLocation(fileSystem, recursiveDirectory, format, location, partitionSpec, Optional.empty(), table.schema());
            addFiles(session, table, dataFiles);
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add files: " + firstNonNull(e.getMessage(), e), e);
        }
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
            return MigrationUtils.buildDataFiles(fileSystem, recursive, format, location, partitionSpec, partition, schema);
        }

        TrinoInputFile file = fileSystem.newInputFile(Location.of(location));
        if (file.exists()) {
            Metrics metrics = loadMetrics(file, format, schema);
            return ImmutableList.of(buildDataFile(file.location().toString(), file.length(), partition, partitionSpec, format.name(), metrics));
        }

        throw new TrinoException(NOT_FOUND, "Location not found: " + location);
    }

    public static void addFilesFromTable(
            ConnectorSession session,
            TrinoFileSystem fileSystem,
            HiveMetastoreFactory metastoreFactory,
            Table targetTable,
            io.trino.metastore.Table sourceTable,
            Map<String, String> partitionFilter,
            RecursiveDirectory recursiveDirectory)
    {
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));

        PartitionSpec partitionSpec = targetTable.spec();
        Schema schema = targetTable.schema();
        NameMapping nameMapping = MappingUtil.create(schema);

        HiveStorageFormat storageFormat = extractHiveStorageFormat(sourceTable.getStorage().getStorageFormat());
        String location = sourceTable.getStorage().getLocation();

        try {
            ImmutableList.Builder<DataFile> dataFilesBuilder = ImmutableList.builder();
            if (partitionSpec.isUnpartitioned()) {
                log.debug("Building data files from %s", location);
                dataFilesBuilder.addAll(buildDataFiles(fileSystem, recursiveDirectory, storageFormat, location, partitionSpec, Optional.empty(), schema));
            }
            else {
                List<String> partitionNames = partitionFilter == null ? ImmutableList.of() : ImmutableList.of(PARTITION_JOINER.join(partitionFilter));
                Map<String, Optional<Partition>> partitions = metastore.getPartitionsByNames(sourceTable, partitionNames);
                for (Map.Entry<String, Optional<Partition>> partition : partitions.entrySet()) {
                    Storage storage = partition.getValue().orElseThrow(() -> new IllegalArgumentException("Invalid partition: " + partition.getKey())).getStorage();
                    log.debug("Building data files from partition: %s", partition);
                    HiveStorageFormat partitionStorageFormat = extractHiveStorageFormat(storage.getStorageFormat());
                    StructLike partitionData = DataFiles.data(partitionSpec, partition.getKey());
                    dataFilesBuilder.addAll(buildDataFiles(fileSystem, recursiveDirectory, partitionStorageFormat, storage.getLocation(), partitionSpec, Optional.of(partitionData), schema));
                }
            }

            log.debug("Start new transaction");
            Transaction transaction = targetTable.newTransaction();
            if (!targetTable.properties().containsKey(DEFAULT_NAME_MAPPING)) {
                log.debug("Update default name mapping property");
                transaction.updateProperties()
                        .set(DEFAULT_NAME_MAPPING, toJson(nameMapping))
                        .commit();
            }
            addFiles(session, targetTable, dataFilesBuilder.build());
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add files: " + firstNonNull(e.getMessage(), e), e);
        }
    }

    public static DataFile buildDataFile(String path, long length, Optional<StructLike> partition, PartitionSpec spec, String format, Metrics metrics)
    {
        DataFiles.Builder dataFile = DataFiles.builder(spec)
                .withPath(path)
                .withFormat(format)
                .withFileSizeInBytes(length)
                .withMetrics(metrics);
        partition.ifPresent(dataFile::withPartition);
        return dataFile.build();
    }

    public static void addFiles(ConnectorSession session, Table table, List<DataFile> dataFiles)
    {
        Schema schema = table.schema();
        Set<Integer> requiredFields = schema.columns().stream()
                .filter(Types.NestedField::isRequired)
                .map(Types.NestedField::fieldId)
                .collect(toImmutableSet());

        ImmutableSet.Builder<String> existingFilesBuilder = ImmutableSet.builder();
        try (CloseableIterable<FileScanTask> iterator = table.newScan().planFiles()) {
            for (FileScanTask fileScanTask : iterator) {
                DataFile dataFile = fileScanTask.file();
                existingFilesBuilder.add(dataFile.location());
            }
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Set<String> existingFiles = existingFilesBuilder.build();

        if (!requiredFields.isEmpty()) {
            for (DataFile dataFile : dataFiles) {
                Map<Integer, Long> nullValueCounts = firstNonNull(dataFile.nullValueCounts(), Map.of());
                for (Integer field : requiredFields) {
                    Long nullCount = nullValueCounts.get(field);
                    if (nullCount == null || nullCount > 0) {
                        throw new TrinoException(CONSTRAINT_VIOLATION, "NULL value not allowed for NOT NULL column: " + schema.findField(field).name());
                    }
                }
            }
        }

        try {
            log.debug("Start new transaction");
            Transaction transaction = table.newTransaction();
            if (!table.properties().containsKey(DEFAULT_NAME_MAPPING)) {
                log.debug("Update default name mapping property");
                transaction.updateProperties()
                        .set(DEFAULT_NAME_MAPPING, toJson(MappingUtil.create(schema)))
                        .commit();
            }
            log.debug("Append data %d data files", dataFiles.size());
            AppendFiles appendFiles = isMergeManifestsOnWrite(session) ? transaction.newAppend() : transaction.newFastAppend();
            for (DataFile dataFile : dataFiles) {
                if (existingFiles.contains(dataFile.location())) {
                    throw new TrinoException(ALREADY_EXISTS, "File already exists: " + dataFile.location());
                }
                appendFiles.appendFile(dataFile);
            }
            appendFiles.commit();
            transaction.commitTransaction();
            log.debug("Successfully added files to %s table", table.name());
        }
        catch (Exception e) {
            throw new TrinoException(ICEBERG_COMMIT_ERROR, "Failed to add files: " + firstNonNull(e.getMessage(), e), e);
        }
    }
}
