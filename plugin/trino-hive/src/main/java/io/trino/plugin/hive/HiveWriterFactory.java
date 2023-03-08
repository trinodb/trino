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
package io.trino.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.event.client.EventClient;
import io.airlift.units.DataSize;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.hdfs.HdfsContext;
import io.trino.hdfs.HdfsEnvironment;
import io.trino.plugin.hive.HiveSessionProperties.InsertExistingPartitionsBehavior;
import io.trino.plugin.hive.LocationService.WriteInfo;
import io.trino.plugin.hive.PartitionUpdate.UpdateMode;
import io.trino.plugin.hive.acid.AcidTransaction;
import io.trino.plugin.hive.metastore.Column;
import io.trino.plugin.hive.metastore.HivePageSinkMetadataProvider;
import io.trino.plugin.hive.metastore.Partition;
import io.trino.plugin.hive.metastore.SortingColumn;
import io.trino.plugin.hive.metastore.StorageFormat;
import io.trino.plugin.hive.metastore.Table;
import io.trino.plugin.hive.orc.OrcFileWriterFactory;
import io.trino.plugin.hive.util.HiveWriteUtils;
import io.trino.spi.NodeManager;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.security.Principal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Maps.immutableEntry;
import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.hdfs.ConfigurationUtils.toJobConf;
import static io.trino.plugin.hive.HiveCompressionCodecs.selectCompressionCodec;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_READ_ONLY;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PARTITION_SCHEMA_MISMATCH;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_TABLE_READ_ONLY;
import static io.trino.plugin.hive.HiveErrorCode.HIVE_UNSUPPORTED_FORMAT;
import static io.trino.plugin.hive.HiveSessionProperties.getInsertExistingPartitionsBehavior;
import static io.trino.plugin.hive.HiveSessionProperties.getTemporaryStagingDirectoryPath;
import static io.trino.plugin.hive.HiveSessionProperties.getTimestampPrecision;
import static io.trino.plugin.hive.HiveSessionProperties.isTemporaryStagingDirectoryEnabled;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.LocationHandle.WriteMode.DIRECT_TO_TARGET_EXISTING_DIRECTORY;
import static io.trino.plugin.hive.acid.AcidOperation.CREATE_TABLE;
import static io.trino.plugin.hive.metastore.MetastoreUtil.getHiveSchema;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.AcidTables.deltaSubdir;
import static io.trino.plugin.hive.util.AcidTables.isFullAcidTable;
import static io.trino.plugin.hive.util.AcidTables.isInsertOnlyTable;
import static io.trino.plugin.hive.util.CompressionConfigUtil.assertCompressionConfigured;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static io.trino.plugin.hive.util.HiveClassNames.HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS;
import static io.trino.plugin.hive.util.HiveUtil.getColumnNames;
import static io.trino.plugin.hive.util.HiveUtil.getColumnTypes;
import static io.trino.plugin.hive.util.HiveUtil.makePartName;
import static io.trino.plugin.hive.util.HiveWriteUtils.createPartitionValues;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMNS;
import static io.trino.plugin.hive.util.SerdeConstants.LIST_COLUMN_TYPES;
import static io.trino.spi.connector.SortOrder.ASC_NULLS_FIRST;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;

public class HiveWriterFactory
{
    private static final int MAX_BUCKET_COUNT = 100_000;
    private static final int BUCKET_NUMBER_PADDING = Integer.toString(MAX_BUCKET_COUNT - 1).length();
    private static final Pattern BUCKET_FROM_FILENAME_PATTERN = Pattern.compile("(0[0-9]+)_.*");

    private final Set<HiveFileWriterFactory> fileWriterFactories;
    private final TrinoFileSystem fileSystem;
    private final String schemaName;
    private final String tableName;
    private final AcidTransaction transaction;
    private final List<HiveColumnHandle> inputColumns;

    private final List<DataColumn> dataColumns;

    private final List<String> partitionColumnNames;
    private final List<Type> partitionColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
    private final HiveStorageFormat partitionStorageFormat;
    private final Map<String, String> additionalTableParameters;
    private final LocationHandle locationHandle;
    private final LocationService locationService;
    private final String queryId;
    private final boolean isCreateTransactionalTable;

    private final HivePageSinkMetadataProvider pageSinkMetadataProvider;
    private final TypeManager typeManager;
    private final PageSorter pageSorter;
    private final JobConf conf;

    private final Table table;
    private final DataSize sortBufferSize;
    private final int maxOpenSortFiles;
    private final boolean sortedWritingTempStagingPathEnabled;
    private final String sortedWritingTempStagingPath;
    private final InsertExistingPartitionsBehavior insertExistingPartitionsBehavior;
    private final DateTimeZone parquetTimeZone;

    private final ConnectorSession session;
    private final OptionalInt bucketCount;
    private final List<SortingColumn> sortedBy;

    private final NodeManager nodeManager;
    private final EventClient eventClient;
    private final Map<String, String> sessionProperties;

    private final HiveWriterStats hiveWriterStats;
    private final Optional<Type> rowType;
    private final Optional<HiveType> hiveRowtype;

    public HiveWriterFactory(
            Set<HiveFileWriterFactory> fileWriterFactories,
            TrinoFileSystemFactory fileSystemFactory,
            String schemaName,
            String tableName,
            boolean isCreateTable,
            AcidTransaction transaction,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            HiveStorageFormat partitionStorageFormat,
            Map<String, String> additionalTableParameters,
            OptionalInt bucketCount,
            List<SortingColumn> sortedBy,
            LocationHandle locationHandle,
            LocationService locationService,
            String queryId,
            HivePageSinkMetadataProvider pageSinkMetadataProvider,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment,
            PageSorter pageSorter,
            DataSize sortBufferSize,
            int maxOpenSortFiles,
            DateTimeZone parquetTimeZone,
            ConnectorSession session,
            NodeManager nodeManager,
            EventClient eventClient,
            HiveSessionProperties hiveSessionProperties,
            HiveWriterStats hiveWriterStats)
    {
        this.fileWriterFactories = ImmutableSet.copyOf(requireNonNull(fileWriterFactories, "fileWriterFactories is null"));
        this.fileSystem = fileSystemFactory.create(session);
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.transaction = requireNonNull(transaction, "transaction is null");
        this.inputColumns = requireNonNull(inputColumns, "inputColumns is null");
        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.partitionStorageFormat = requireNonNull(partitionStorageFormat, "partitionStorageFormat is null");
        this.additionalTableParameters = ImmutableMap.copyOf(requireNonNull(additionalTableParameters, "additionalTableParameters is null"));
        this.locationHandle = requireNonNull(locationHandle, "locationHandle is null");
        this.locationService = requireNonNull(locationService, "locationService is null");
        this.queryId = requireNonNull(queryId, "queryId is null");

        this.pageSinkMetadataProvider = requireNonNull(pageSinkMetadataProvider, "pageSinkMetadataProvider is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        this.pageSorter = requireNonNull(pageSorter, "pageSorter is null");
        this.sortBufferSize = requireNonNull(sortBufferSize, "sortBufferSize is null");
        this.maxOpenSortFiles = maxOpenSortFiles;
        this.sortedWritingTempStagingPathEnabled = isTemporaryStagingDirectoryEnabled(session);
        this.sortedWritingTempStagingPath = getTemporaryStagingDirectoryPath(session);
        this.insertExistingPartitionsBehavior = getInsertExistingPartitionsBehavior(session);
        this.parquetTimeZone = requireNonNull(parquetTimeZone, "parquetTimeZone is null");

        // divide input columns into partition and data columns
        ImmutableList.Builder<String> partitionColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> partitionColumnTypes = ImmutableList.builder();
        ImmutableList.Builder<DataColumn> dataColumns = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            HiveType hiveType = column.getHiveType();
            if (column.isPartitionKey()) {
                partitionColumnNames.add(column.getName());
                partitionColumnTypes.add(column.getType());
            }
            else {
                dataColumns.add(new DataColumn(column.getName(), hiveType));
            }
        }
        if (transaction.isMerge()) {
            Type mergeRowType = RowType.from(inputColumns.stream()
                    .filter(column -> !column.isPartitionKey())
                    .map(column -> new RowType.Field(Optional.of(column.getName()), column.getType()))
                    .collect(toImmutableList()));
            this.rowType = Optional.of(mergeRowType);
            this.hiveRowtype = Optional.of(toHiveType(mergeRowType));
        }
        else {
            this.rowType = Optional.empty();
            this.hiveRowtype = Optional.empty();
        }
        this.partitionColumnNames = partitionColumnNames.build();
        this.partitionColumnTypes = partitionColumnTypes.build();
        this.dataColumns = dataColumns.build();
        this.isCreateTransactionalTable = isCreateTable && transaction.isTransactional();

        Path writePath;
        if (isCreateTable) {
            this.table = null;
            WriteInfo writeInfo = locationService.getQueryWriteInfo(locationHandle);
            checkArgument(writeInfo.getWriteMode() != DIRECT_TO_TARGET_EXISTING_DIRECTORY, "CREATE TABLE write mode cannot be DIRECT_TO_TARGET_EXISTING_DIRECTORY");
            writePath = writeInfo.getWritePath();
        }
        else {
            this.table = pageSinkMetadataProvider.getTable()
                    .orElseThrow(() -> new TrinoException(HIVE_INVALID_METADATA, format("Table '%s.%s' was dropped during insert", schemaName, tableName)));
            writePath = locationService.getQueryWriteInfo(locationHandle).getWritePath();
        }

        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
        if (bucketCount.isPresent()) {
            checkArgument(bucketCount.getAsInt() < MAX_BUCKET_COUNT, "bucketCount must be smaller than %s", MAX_BUCKET_COUNT);
        }

        this.sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));

        this.session = requireNonNull(session, "session is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.eventClient = requireNonNull(eventClient, "eventClient is null");

        requireNonNull(hiveSessionProperties, "hiveSessionProperties is null");
        this.sessionProperties = hiveSessionProperties.getSessionProperties().stream()
                .map(propertyMetadata -> immutableEntry(
                        propertyMetadata.getName(),
                        session.getProperty(propertyMetadata.getName(), propertyMetadata.getJavaType())))
                // The session properties collected here are used for events only. Filter out nulls to avoid problems with downstream consumers
                .filter(entry -> entry.getValue() != null)
                .collect(toImmutableMap(Entry::getKey, entry -> entry.getValue().toString()));

        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsContext(session), writePath);
        this.conf = toJobConf(conf);

        // make sure the FileSystem is created with the correct Configuration object
        try {
            hdfsEnvironment.getFileSystem(session.getIdentity(), writePath, conf);
        }
        catch (IOException e) {
            throw new TrinoException(HIVE_FILESYSTEM_ERROR, "Failed getting FileSystem: " + writePath, e);
        }

        this.hiveWriterStats = requireNonNull(hiveWriterStats, "hiveWriterStats is null");
    }

    public HiveWriter createWriter(Page partitionColumns, int position, OptionalInt bucketNumber)
    {
        if (bucketCount.isPresent()) {
            checkArgument(bucketNumber.isPresent(), "Bucket not provided for bucketed table");
            checkArgument(bucketNumber.getAsInt() < bucketCount.getAsInt(), "Bucket number %s must be less than bucket count %s", bucketNumber, bucketCount);
        }
        else {
            checkArgument(bucketNumber.isEmpty(), "Bucket number provided by for table that is not bucketed");
        }

        List<String> partitionValues = createPartitionValues(partitionColumnTypes, partitionColumns, position);

        Optional<String> partitionName;
        if (!partitionColumnNames.isEmpty()) {
            partitionName = Optional.of(makePartName(partitionColumnNames, partitionValues));
        }
        else {
            partitionName = Optional.empty();
        }

        // attempt to get the existing partition (if this is an existing partitioned table)
        Optional<Partition> partition = Optional.empty();
        if (!partitionValues.isEmpty() && table != null) {
            partition = pageSinkMetadataProvider.getPartition(partitionValues);
        }

        UpdateMode updateMode;
        Properties schema;
        WriteInfo writeInfo;
        StorageFormat outputStorageFormat;
        JobConf outputConf = new JobConf(conf);
        if (partition.isEmpty()) {
            if (table == null) {
                // Write to: a new partition in a new partitioned table,
                //           or a new unpartitioned table.
                updateMode = UpdateMode.NEW;
                schema = new Properties();
                schema.setProperty(LIST_COLUMNS, dataColumns.stream()
                        .map(DataColumn::getName)
                        .collect(joining(",")));
                schema.setProperty(LIST_COLUMN_TYPES, dataColumns.stream()
                        .map(DataColumn::getHiveType)
                        .map(HiveType::getHiveTypeName)
                        .map(HiveTypeName::toString)
                        .collect(joining(":")));

                if (partitionName.isEmpty()) {
                    // new unpartitioned table
                    writeInfo = locationService.getTableWriteInfo(locationHandle, false);
                }
                else {
                    // a new partition in a new partitioned table
                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());

                    if (!writeInfo.getWriteMode().isWritePathSameAsTargetPath()) {
                        // When target path is different from write path,
                        // verify that the target directory for the partition does not already exist
                        String writeInfoTargetPath = writeInfo.getTargetPath().toString();
                        try {
                            if (fileSystem.newInputFile(writeInfoTargetPath).exists()) {
                                throw new TrinoException(HIVE_PATH_ALREADY_EXISTS, format(
                                        "Target directory for new partition '%s' of table '%s.%s' already exists: %s",
                                        partitionName,
                                        schemaName,
                                        tableName,
                                        writeInfo.getTargetPath()));
                            }
                        }
                        catch (IOException e) {
                            throw new TrinoException(HIVE_FILESYSTEM_ERROR, format("Error while accessing: %s", writeInfoTargetPath), e);
                        }
                    }
                }
            }
            else {
                // Write to: a new partition in an existing partitioned table,
                //           or an existing unpartitioned table
                if (partitionName.isPresent()) {
                    // a new partition in an existing partitioned table
                    updateMode = UpdateMode.NEW;
                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());
                }
                else {
                    switch (insertExistingPartitionsBehavior) {
                        case APPEND:
                            updateMode = UpdateMode.APPEND;
                            writeInfo = locationService.getTableWriteInfo(locationHandle, false);
                            break;
                        case OVERWRITE:
                            updateMode = UpdateMode.OVERWRITE;
                            writeInfo = locationService.getTableWriteInfo(locationHandle, true);
                            break;
                        case ERROR:
                            throw new TrinoException(HIVE_TABLE_READ_ONLY, "Unpartitioned Hive tables are immutable");
                        default:
                            throw new IllegalArgumentException("Unsupported insert existing table behavior: " + insertExistingPartitionsBehavior);
                    }
                }

                schema = getHiveSchema(table);
            }

            if (partitionName.isPresent()) {
                // Write to a new partition
                outputStorageFormat = fromHiveStorageFormat(partitionStorageFormat);
                configureCompression(outputConf, selectCompressionCodec(session, partitionStorageFormat));
            }
            else {
                // Write to a new/existing unpartitioned table
                outputStorageFormat = fromHiveStorageFormat(tableStorageFormat);
                configureCompression(outputConf, selectCompressionCodec(session, tableStorageFormat));
            }
        }
        else {
            switch (insertExistingPartitionsBehavior) {
                // Write to: an existing partition in an existing partitioned table
                case APPEND:
                    // Append to an existing partition
                    updateMode = UpdateMode.APPEND;
                    // Check the column types in partition schema match the column types in table schema
                    List<Column> tableColumns = table.getDataColumns();
                    List<Column> existingPartitionColumns = partition.get().getColumns();
                    for (int i = 0; i < min(existingPartitionColumns.size(), tableColumns.size()); i++) {
                        HiveType tableType = tableColumns.get(i).getType();
                        HiveType partitionType = existingPartitionColumns.get(i).getType();
                        if (!tableType.equals(partitionType)) {
                            throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format("" +
                                            "You are trying to write into an existing partition in a table. " +
                                            "The table schema has changed since the creation of the partition. " +
                                            "Inserting rows into such partition is not supported. " +
                                            "The column '%s' in table '%s' is declared as type '%s', " +
                                            "but partition '%s' declared column '%s' as type '%s'.",
                                    tableColumns.get(i).getName(),
                                    tableName,
                                    tableType,
                                    partitionName,
                                    existingPartitionColumns.get(i).getName(),
                                    partitionType));
                        }
                    }

                    HiveWriteUtils.checkPartitionIsWritable(partitionName.get(), partition.get());

                    outputStorageFormat = partition.get().getStorage().getStorageFormat();
                    configureCompression(outputConf, selectCompressionCodec(session, outputStorageFormat));
                    schema = getHiveSchema(partition.get(), table);

                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, partition, partitionName.get());
                    break;
                case OVERWRITE:
                    // Overwrite an existing partition
                    //
                    // The behavior of overwrite considered as if first dropping the partition and inserting a new partition, thus:
                    // * No partition writable check is required.
                    // * Table schema and storage format is used for the new partition (instead of existing partition schema and storage format).
                    updateMode = UpdateMode.OVERWRITE;

                    outputStorageFormat = fromHiveStorageFormat(partitionStorageFormat);
                    configureCompression(outputConf, selectCompressionCodec(session, partitionStorageFormat));
                    schema = getHiveSchema(table);

                    writeInfo = locationService.getPartitionWriteInfo(locationHandle, Optional.empty(), partitionName.get());
                    break;
                case ERROR:
                    throw new TrinoException(HIVE_PARTITION_READ_ONLY, "Cannot insert into an existing partition of Hive table: " + partitionName.get());
                default:
                    throw new IllegalArgumentException(format("Unsupported insert existing partitions behavior: %s", insertExistingPartitionsBehavior));
            }
        }

        // verify compression was properly set by each of code paths above
        assertCompressionConfigured(outputConf);

        additionalTableParameters.forEach(schema::setProperty);

        validateSchema(partitionName, schema);

        int bucketToUse = bucketNumber.isEmpty() ? 0 : bucketNumber.getAsInt();

        Path path;
        String fileNameWithExtension;
        if (transaction.isAcidTransactionRunning() && transaction.getOperation() != CREATE_TABLE) {
            String subdir = computeAcidSubdir(transaction);
            Path subdirPath = new Path(writeInfo.getWritePath(), subdir);
            String nameFormat = table != null && isInsertOnlyTable(table.getParameters()) ? "%05d_0" : "bucket_%05d";
            path = new Path(subdirPath, format(nameFormat, bucketToUse));
            fileNameWithExtension = path.getName();
        }
        else {
            String fileName = computeFileName(bucketNumber);
            fileNameWithExtension = fileName + getFileExtension(outputConf, outputStorageFormat);
            path = new Path(writeInfo.getWritePath(), fileNameWithExtension);
        }

        boolean useAcidSchema = isCreateTransactionalTable || (table != null && isFullAcidTable(table.getParameters()));

        FileWriter hiveFileWriter = null;

        if (transaction.isMerge()) {
            OrcFileWriterFactory orcFileWriterFactory = (OrcFileWriterFactory) fileWriterFactories.stream()
                    .filter(factory -> factory instanceof OrcFileWriterFactory)
                    .collect(onlyElement());
            checkArgument(hiveRowtype.isPresent(), "rowTypes not present");
            RowIdSortingFileWriterMaker fileWriterMaker = (deleteWriter, deletePath) -> makeRowIdSortingWriter(deleteWriter, deletePath);
            hiveFileWriter = new MergeFileWriter(transaction, 0, bucketNumber, fileWriterMaker, path, orcFileWriterFactory, inputColumns, conf, session, typeManager, hiveRowtype.get());
        }
        else {
            for (HiveFileWriterFactory fileWriterFactory : fileWriterFactories) {
                Optional<FileWriter> fileWriter = fileWriterFactory.createFileWriter(
                        path,
                        dataColumns.stream()
                                .map(DataColumn::getName)
                                .collect(toList()),
                        outputStorageFormat,
                        schema,
                        outputConf,
                        session,
                        bucketNumber,
                        transaction,
                        useAcidSchema,
                        WriterKind.INSERT);

                if (fileWriter.isPresent()) {
                    hiveFileWriter = fileWriter.get();
                    break;
                }
            }
        }

        if (hiveFileWriter == null) {
            hiveFileWriter = new RecordFileWriter(
                    path,
                    dataColumns.stream()
                            .map(DataColumn::getName)
                            .collect(toList()),
                    outputStorageFormat,
                    schema,
                    partitionStorageFormat.getEstimatedWriterMemoryUsage(),
                    outputConf,
                    typeManager,
                    parquetTimeZone,
                    session);
        }

        String writerImplementation = hiveFileWriter.getClass().getName();

        Consumer<HiveWriter> onCommit = hiveWriter -> {
            Optional<Long> size;
            try {
                size = Optional.of(hiveWriter.getWrittenBytes());
            }
            catch (RuntimeException e) {
                // Do not fail the query if file system is not available
                size = Optional.empty();
            }

            eventClient.post(new WriteCompletedEvent(
                    session.getQueryId(),
                    path.toString(),
                    schemaName,
                    tableName,
                    partitionName.orElse(null),
                    outputStorageFormat.getOutputFormat(),
                    writerImplementation,
                    nodeManager.getCurrentNode().getVersion(),
                    nodeManager.getCurrentNode().getHost(),
                    session.getIdentity().getPrincipal().map(Principal::getName).orElse(null),
                    nodeManager.getEnvironment(),
                    sessionProperties,
                    size.orElse(null),
                    hiveWriter.getRowCount()));
        };

        if (!sortedBy.isEmpty()) {
            Path tempFilePath;
            if (sortedWritingTempStagingPathEnabled) {
                String tempPrefix = sortedWritingTempStagingPath.replace(
                        "${USER}",
                        new HdfsContext(session).getIdentity().getUser());
                tempPrefix = setSchemeToFileIfAbsent(tempPrefix);
                tempFilePath = new Path(tempPrefix, ".tmp-sort." + path.getParent().getName() + "." + path.getName());
            }
            else {
                tempFilePath = new Path(path.getParent(), ".tmp-sort." + path.getName());
            }

            List<Type> types = dataColumns.stream()
                    .map(column -> column.getHiveType().getType(typeManager, getTimestampPrecision(session)))
                    .collect(toImmutableList());

            Map<String, Integer> columnIndexes = new HashMap<>();
            for (int i = 0; i < dataColumns.size(); i++) {
                columnIndexes.put(dataColumns.get(i).getName(), i);
            }

            List<Integer> sortFields = new ArrayList<>();
            List<SortOrder> sortOrders = new ArrayList<>();
            for (SortingColumn column : sortedBy) {
                Integer index = columnIndexes.get(column.getColumnName());
                if (index == null) {
                    throw new TrinoException(HIVE_INVALID_METADATA, format("Sorting column '%s' does exist in table '%s.%s'", column.getColumnName(), schemaName, tableName));
                }
                sortFields.add(index);
                sortOrders.add(column.getOrder().getSortOrder());
            }

            hiveFileWriter = new SortingFileWriter(
                    fileSystem,
                    tempFilePath.toString(),
                    hiveFileWriter,
                    sortBufferSize,
                    maxOpenSortFiles,
                    types,
                    sortFields,
                    sortOrders,
                    pageSorter,
                    typeManager.getTypeOperators(),
                    OrcFileWriterFactory::createOrcDataSink);
        }

        return new HiveWriter(
                hiveFileWriter,
                partitionName,
                updateMode,
                fileNameWithExtension,
                writeInfo.getWritePath().toString(),
                writeInfo.getTargetPath().toString(),
                onCommit,
                hiveWriterStats);
    }

    public interface RowIdSortingFileWriterMaker
    {
        SortingFileWriter makeFileWriter(FileWriter deleteFileWriter, Path path);
    }

    public SortingFileWriter makeRowIdSortingWriter(FileWriter deleteFileWriter, Path path)
    {
        String parentPath = setSchemeToFileIfAbsent(path.getParent().toString());
        Path tempFilePath = new Path(parentPath, ".tmp-sort." + path.getName());
        // The ORC columns are: operation, originalTransaction, bucket, rowId, row
        // The deleted rows should be sorted by originalTransaction, then by rowId
        List<Integer> sortFields = ImmutableList.of(1, 3);
        List<SortOrder> sortOrders = ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST);
        // The types are indexed by sortField in the SortFileWriter stack
        List<Type> types = ImmutableList.of(INTEGER, BIGINT, INTEGER, BIGINT, BIGINT, rowType.get());

        return new SortingFileWriter(
                fileSystem,
                tempFilePath.toString(),
                deleteFileWriter,
                sortBufferSize,
                maxOpenSortFiles,
                types,
                sortFields,
                sortOrders,
                pageSorter,
                typeManager.getTypeOperators(),
                OrcFileWriterFactory::createOrcDataSink);
    }

    private void validateSchema(Optional<String> partitionName, Properties schema)
    {
        // existing tables may have columns in a different order
        List<String> fileColumnNames = getColumnNames(schema);
        List<HiveType> fileColumnHiveTypes = getColumnTypes(schema);

        // verify we can write all input columns to the file
        Map<String, DataColumn> inputColumnMap = dataColumns.stream()
                .collect(toMap(DataColumn::getName, identity()));
        Set<String> missingColumns = Sets.difference(inputColumnMap.keySet(), new HashSet<>(fileColumnNames));
        if (!missingColumns.isEmpty()) {
            throw new TrinoException(HIVE_INVALID_METADATA, format("Table '%s.%s' does not have columns %s", schemaName, tableName, missingColumns));
        }
        if (fileColumnNames.size() != fileColumnHiveTypes.size()) {
            throw new TrinoException(HIVE_INVALID_METADATA, format(
                    "Partition '%s' in table '%s.%s' has mismatched metadata for column names and types",
                    partitionName.orElse(""), // TODO: this should exist
                    schemaName,
                    tableName));
        }

        // verify the file types match the input type
        // todo adapt input types to the file types as Hive does
        for (int fileIndex = 0; fileIndex < fileColumnNames.size(); fileIndex++) {
            String columnName = fileColumnNames.get(fileIndex);
            HiveType fileColumnHiveType = fileColumnHiveTypes.get(fileIndex);
            HiveType inputHiveType = inputColumnMap.get(columnName).getHiveType();

            if (!fileColumnHiveType.equals(inputHiveType)) {
                // todo this should be moved to a helper
                throw new TrinoException(HIVE_PARTITION_SCHEMA_MISMATCH, format(
                        "" +
                                "There is a mismatch between the table and partition schemas. " +
                                "The column '%s' in table '%s.%s' is declared as type '%s', " +
                                "but partition '%s' declared column '%s' as type '%s'.",
                        columnName,
                        schemaName,
                        tableName,
                        inputHiveType,
                        partitionName.orElse(""), // TODO: this should exist
                        columnName,
                        fileColumnHiveType));
            }
        }
    }

    private String computeAcidSubdir(AcidTransaction transaction)
    {
        long writeId = transaction.getWriteId();
        switch (transaction.getOperation()) {
            case INSERT:
            case CREATE_TABLE:
            case MERGE:
                return deltaSubdir(writeId, 0);
            default:
                throw new UnsupportedOperationException("transaction operation is " + transaction.getOperation());
        }
    }

    private String computeFileName(OptionalInt bucketNumber)
    {
        // Currently CTAS for transactional tables in Trino creates non-transactional ("original") files.
        // Hive requires "original" files of transactional tables to conform to the following naming pattern:
        //
        // For bucketed tables we drop query id from file names and just leave <bucketId>_0
        // For non bucketed tables we use 000000_<uuid_as_number>

        if (bucketNumber.isPresent()) {
            if (isCreateTransactionalTable) {
                return computeTransactionalBucketedFilename(bucketNumber.getAsInt());
            }
            return computeNonTransactionalBucketedFilename(queryId, bucketNumber.getAsInt());
        }

        if (isCreateTransactionalTable) {
            String paddedBucket = Strings.padStart("0", BUCKET_NUMBER_PADDING, '0');
            UUID uuid = randomUUID();
            return format("0%s_%s%s",
                    paddedBucket,
                    Long.toUnsignedString(uuid.getLeastSignificantBits()),
                    Long.toUnsignedString(uuid.getMostSignificantBits()));
        }

        return queryId + "_" + randomUUID();
    }

    public static String computeNonTransactionalBucketedFilename(String queryId, int bucket)
    {
        // It is important that we put query id at the end of suffix which we use to compute the file name.
        // Filename must either start or end with query id so HiveWriteUtils.isFileCreatedByQuery works correctly.
        return computeBucketedFileName(Optional.of(randomUUID() + "_" + queryId), bucket);
    }

    public static String computeTransactionalBucketedFilename(int bucket)
    {
        return computeBucketedFileName(Optional.empty(), bucket);
    }

    private static String computeBucketedFileName(Optional<String> suffix, int bucket)
    {
        String paddedBucket = Strings.padStart(Integer.toString(bucket), BUCKET_NUMBER_PADDING, '0');
        if (suffix.isPresent()) {
            return format("0%s_0_%s", paddedBucket, suffix.get());
        }
        return format("0%s_0", paddedBucket);
    }

    public static int getBucketFromFileName(String fileName)
    {
        Matcher matcher = BUCKET_FROM_FILENAME_PATTERN.matcher(fileName);
        checkArgument(matcher.matches(), "filename %s does not match pattern %s", fileName, BUCKET_FROM_FILENAME_PATTERN);
        return Integer.parseInt(matcher.group(1));
    }

    public static String getFileExtension(JobConf conf, StorageFormat storageFormat)
    {
        // text format files must have the correct extension when compressed
        if (!HiveConf.getBoolVar(conf, COMPRESSRESULT) || !HIVE_IGNORE_KEY_OUTPUT_FORMAT_CLASS.equals(storageFormat.getOutputFormat())) {
            return "";
        }

        String compressionCodecClass = conf.get("mapred.output.compression.codec");
        if (compressionCodecClass == null) {
            return new DefaultCodec().getDefaultExtension();
        }

        try {
            Class<? extends CompressionCodec> codecClass = conf.getClassByName(compressionCodecClass).asSubclass(CompressionCodec.class);
            return ReflectionUtils.newInstance(codecClass, conf).getDefaultExtension();
        }
        catch (ClassNotFoundException e) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Compression codec not found: " + compressionCodecClass, e);
        }
        catch (RuntimeException e) {
            throw new TrinoException(HIVE_UNSUPPORTED_FORMAT, "Failed to load compression codec: " + compressionCodecClass, e);
        }
    }

    @VisibleForTesting
    static String setSchemeToFileIfAbsent(String pathString)
    {
        Path path = new Path(pathString);
        String scheme = path.toUri().getScheme();
        if (scheme == null || scheme.equals("")) {
            return "file:///" + pathString;
        }
        return pathString;
    }

    private static class DataColumn
    {
        private final String name;
        private final HiveType hiveType;

        public DataColumn(String name, HiveType hiveType)
        {
            this.name = requireNonNull(name, "name is null");
            this.hiveType = requireNonNull(hiveType, "hiveType is null");
        }

        public String getName()
        {
            return name;
        }

        public HiveType getHiveType()
        {
            return hiveType;
        }
    }
}
