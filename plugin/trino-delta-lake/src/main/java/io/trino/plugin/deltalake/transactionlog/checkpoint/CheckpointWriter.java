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
package io.trino.plugin.deltalake.transactionlog.checkpoint;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.DeltaHiveTypeTranslator;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.plugin.hive.HdfsEnvironment;
import io.trino.plugin.hive.HiveType;
import io.trino.plugin.hive.HiveTypeName;
import io.trino.plugin.hive.RecordFileWriter;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTimeZone;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveCompressionCodec.SNAPPY;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.metastore.StorageFormat.fromHiveStorageFormat;
import static io.trino.plugin.hive.util.CompressionConfigUtil.configureCompression;
import static io.trino.plugin.hive.util.ConfigurationUtils.toJobConf;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

public class CheckpointWriter
{
    private static final int METADATA_BLOCK_CHANNEL = 0;
    private static final int PROTOCOL_BLOCK_CHANNEL = 1;
    private static final int TXN_BLOCK_CHANNEL = 2;
    private static final int ADD_BLOCK_CHANNEL = 3;
    private static final int REMOVE_BLOCK_CHANNEL = 4;

    // must match channel list above
    private static final int CHANNELS_COUNT = 5;

    private final TypeManager typeManager;
    private final CheckpointSchemaManager checkpointSchemaManager;
    private final HdfsEnvironment hdfsEnvironment;

    public CheckpointWriter(TypeManager typeManager, CheckpointSchemaManager checkpointSchemaManager, HdfsEnvironment hdfsEnvironment)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
    }

    public void write(ConnectorSession session, CheckpointEntries entries, Path targetPath)
    {
        RowType metadataEntryType = checkpointSchemaManager.getMetadataEntryType();
        RowType protocolEntryType = checkpointSchemaManager.getProtocolEntryType();
        RowType txnEntryType = checkpointSchemaManager.getTxnEntryType();
        RowType addEntryType = checkpointSchemaManager.getAddEntryType(entries.getMetadataEntry());
        RowType removeEntryType = checkpointSchemaManager.getRemoveEntryType();

        List<String> columnNames = ImmutableList.of(
                "metaData",
                "protocol",
                "txn",
                "add",
                "remove");
        List<Type> columnTypes = ImmutableList.of(
                metadataEntryType,
                protocolEntryType,
                txnEntryType,
                addEntryType,
                removeEntryType);

        Properties schema = buildSchemaProperties(columnNames, columnTypes);

        Configuration conf = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session), targetPath);
        configureCompression(conf, SNAPPY);
        JobConf jobConf = toJobConf(conf);

        RecordFileWriter writer = new RecordFileWriter(
                targetPath,
                columnNames,
                fromHiveStorageFormat(PARQUET),
                schema,
                PARQUET.getEstimatedWriterMemoryUsage(),
                jobConf,
                typeManager,
                DateTimeZone.UTC,
                session);

        PageBuilder pageBuilder = new PageBuilder(columnTypes);

        writeMetadataEntry(pageBuilder, metadataEntryType, entries.getMetadataEntry());
        writeProtocolEntry(pageBuilder, protocolEntryType, entries.getProtocolEntry());
        for (TransactionEntry transactionEntry : entries.getTransactionEntries()) {
            writeTransactionEntry(pageBuilder, txnEntryType, transactionEntry);
        }
        for (AddFileEntry addFileEntry : entries.getAddFileEntries()) {
            writeAddFileEntry(pageBuilder, addEntryType, addFileEntry);
        }
        for (RemoveFileEntry removeFileEntry : entries.getRemoveFileEntries()) {
            writeRemoveFileEntry(pageBuilder, removeEntryType, removeFileEntry);
        }
        // Not writing commit infos for now. DB does not keep them in the checkpoints by default

        writer.appendRows(pageBuilder.build());
        writer.commit();
    }

    private static Properties buildSchemaProperties(List<String> columnNames, List<Type> columnTypes)
    {
        // TODO copied out from DeltaLakePageSink. Extrat to utility class
        Properties schema = new Properties();
        schema.setProperty(IOConstants.COLUMNS, String.join(",", columnNames));
        schema.setProperty(IOConstants.COLUMNS_TYPES, columnTypes.stream()
                .map(DeltaHiveTypeTranslator::toHiveType)
                .map(HiveType::getHiveTypeName)
                .map(HiveTypeName::toString)
                .collect(joining(":")));
        return schema;
    }

    private void writeMetadataEntry(PageBuilder pageBuilder, RowType entryType, MetadataEntry metadataEntry)
    {
        pageBuilder.declarePosition();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(METADATA_BLOCK_CHANNEL);
        BlockBuilder entryBlockBuilder = blockBuilder.beginBlockEntry();
        writeString(entryBlockBuilder, entryType, 0, "id", metadataEntry.getId());
        writeString(entryBlockBuilder, entryType, 1, "name", metadataEntry.getName());
        writeString(entryBlockBuilder, entryType, 2, "description", metadataEntry.getDescription());

        RowType formatType = getInternalRowType(entryType, 3, "format");
        BlockBuilder formatBlockBuilder = entryBlockBuilder.beginBlockEntry();
        writeString(formatBlockBuilder, formatType, 0, "provider", metadataEntry.getFormat().getProvider());
        writeStringMap(formatBlockBuilder, formatType, 1, "options", metadataEntry.getFormat().getOptions());
        entryBlockBuilder.closeEntry();

        writeString(entryBlockBuilder, entryType, 4, "schemaString", metadataEntry.getSchemaString());
        writeStringList(entryBlockBuilder, entryType, 5, "partitionColumns", metadataEntry.getOriginalPartitionColumns());
        writeStringMap(entryBlockBuilder, entryType, 6, "configuration", metadataEntry.getConfiguration());
        writeLong(entryBlockBuilder, entryType, 7, "createdTime", metadataEntry.getCreatedTime());
        blockBuilder.closeEntry();

        // null for others
        appendNullOtherBlocks(pageBuilder, METADATA_BLOCK_CHANNEL);
    }

    private void writeProtocolEntry(PageBuilder pageBuilder, RowType entryType, ProtocolEntry protocolEntry)
    {
        pageBuilder.declarePosition();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(PROTOCOL_BLOCK_CHANNEL);
        BlockBuilder entryBlockBuilder = blockBuilder.beginBlockEntry();
        writeLong(entryBlockBuilder, entryType, 0, "minReaderVersion", (long) protocolEntry.getMinReaderVersion());
        writeLong(entryBlockBuilder, entryType, 1, "minWriterVersion", (long) protocolEntry.getMinWriterVersion());
        blockBuilder.closeEntry();

        // null for others
        appendNullOtherBlocks(pageBuilder, PROTOCOL_BLOCK_CHANNEL);
    }

    private void writeTransactionEntry(PageBuilder pageBuilder, RowType entryType, TransactionEntry transactionEntry)
    {
        pageBuilder.declarePosition();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(TXN_BLOCK_CHANNEL);
        BlockBuilder entryBlockBuilder = blockBuilder.beginBlockEntry();
        writeString(entryBlockBuilder, entryType, 0, "appId", transactionEntry.getAppId());
        writeLong(entryBlockBuilder, entryType, 1, "version", transactionEntry.getVersion());
        writeLong(entryBlockBuilder, entryType, 2, "lastUpdated", transactionEntry.getLastUpdated());
        blockBuilder.closeEntry();

        // null for others
        appendNullOtherBlocks(pageBuilder, TXN_BLOCK_CHANNEL);
    }

    private void writeAddFileEntry(PageBuilder pageBuilder, RowType entryType, AddFileEntry addFileEntry)
    {
        pageBuilder.declarePosition();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(ADD_BLOCK_CHANNEL);
        BlockBuilder entryBlockBuilder = blockBuilder.beginBlockEntry();
        writeString(entryBlockBuilder, entryType, 0, "path", addFileEntry.getPath());
        writeStringMap(entryBlockBuilder, entryType, 1, "partitionValues", addFileEntry.getPartitionValues());
        writeLong(entryBlockBuilder, entryType, 2, "size", addFileEntry.getSize());
        writeLong(entryBlockBuilder, entryType, 3, "modificationTime", addFileEntry.getModificationTime());
        writeBoolean(entryBlockBuilder, entryType, 4, "dataChange", addFileEntry.isDataChange());
        // TODO: determine stats format in checkpoint based on table configuration;
        // currently if addFileEntry contains JSON stats we will write JSON
        // stats to checkpoint and if addEntryFile contains parsed stats, we
        // will write parsed stats to the checkpoint.
        writeJsonStats(entryBlockBuilder, entryType, addFileEntry);
        writeParsedStats(entryBlockBuilder, entryType, addFileEntry);
        writeStringMap(entryBlockBuilder, entryType, 7, "tags", addFileEntry.getTags());
        blockBuilder.closeEntry();

        // null for others
        appendNullOtherBlocks(pageBuilder, ADD_BLOCK_CHANNEL);
    }

    private void writeJsonStats(BlockBuilder entryBlockBuilder, RowType entryType, AddFileEntry addFileEntry)
    {
        String statsJson = null;
        if (addFileEntry.getStats().isPresent() && addFileEntry.getStats().get() instanceof DeltaLakeJsonFileStatistics) {
            statsJson = addFileEntry.getStatsString().orElse(null);
        }
        writeString(entryBlockBuilder, entryType, 5, "stats", statsJson);
    }

    private void writeParsedStats(BlockBuilder entryBlockBuilder, RowType entryType, AddFileEntry addFileEntry)
    {
        RowType statsType = getInternalRowType(entryType, 6, "stats_parsed");
        if (addFileEntry.getStats().isEmpty() || !(addFileEntry.getStats().get() instanceof DeltaLakeParquetFileStatistics)) {
            entryBlockBuilder.appendNull();
            return;
        }
        DeltaLakeParquetFileStatistics stats = (DeltaLakeParquetFileStatistics) addFileEntry.getStats().get();
        BlockBuilder statsBlockBuilder = entryBlockBuilder.beginBlockEntry();

        writeLong(statsBlockBuilder, statsType, 0, "numRecords", stats.getNumRecords().orElse(null));
        writeMinMaxMapAsFields(statsBlockBuilder, statsType, 1, "minValues", stats.getMinValues());
        writeMinMaxMapAsFields(statsBlockBuilder, statsType, 2, "maxValues", stats.getMaxValues());
        writeObjectMapAsFields(statsBlockBuilder, statsType, 3, "nullCount", stats.getNullCount());
        entryBlockBuilder.closeEntry();
    }

    private void writeMinMaxMapAsFields(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, Optional<Map<String, Object>> values)
    {
        RowType.Field valuesField = validateAndGetField(type, fieldId, fieldName);
        RowType valuesFieldType = (RowType) valuesField.getType();
        writeObjectMapAsFields(blockBuilder, type, fieldId, fieldName, preprocessMinMaxValues(valuesFieldType, values));
    }

    private void writeObjectMapAsFields(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, Optional<Map<String, Object>> values)
    {
        RowType.Field valuesField = validateAndGetField(type, fieldId, fieldName);
        RowType valuesFieldType = (RowType) valuesField.getType();
        BlockBuilder fieldBlockBuilder = blockBuilder.beginBlockEntry();
        if (values.isEmpty()) {
            blockBuilder.appendNull();
        }
        else {
            for (RowType.Field valueField : valuesFieldType.getFields()) {
                // anonymous row fields are not expected here
                Object value = values.get().get(valueField.getName().orElseThrow());
                if (valueField.getType() instanceof RowType) {
                    Block rowBlock = (Block) value;
                    checkState(rowBlock.getPositionCount() == 1, "Invalid RowType statistics for writing Delta Lake checkpoint");
                    if (rowBlock.isNull(0)) {
                        fieldBlockBuilder.appendNull();
                    }
                    else {
                        valueField.getType().appendTo(rowBlock, 0, fieldBlockBuilder);
                    }
                }
                else {
                    writeNativeValue(valueField.getType(), fieldBlockBuilder, value);
                }
            }
        }
        blockBuilder.closeEntry();
    }

    private Optional<Map<String, Object>> preprocessMinMaxValues(RowType valuesType, Optional<Map<String, Object>> valuesOptional)
    {
        return valuesOptional.map(
                values -> {
                    Map<String, Type> fieldTypes = valuesType.getFields().stream().collect(toMap(
                            // anonymous row fields are not expected here
                            field -> field.getName().orElseThrow(),
                            RowType.Field::getType));

                    return values.entrySet().stream()
                            .collect(toMap(
                                    Map.Entry::getKey,
                                    entry -> {
                                        Type type = fieldTypes.get(entry.getKey());
                                        Object value = entry.getValue();
                                        if (type instanceof TimestampType) {
                                            // We need to remap TIMESTAMP WITH TIME ZONE -> TIMESTAMP here because of
                                            // inconsistency in what type is used for DL "timestamp" type in data processing and in min/max statistics map.
                                            value = multiplyExact(DateTimeEncoding.unpackMillisUtc((long) value), MICROSECONDS_PER_MILLISECOND);
                                        }
                                        return value;
                                    }));
                });
    }

    private void writeRemoveFileEntry(PageBuilder pageBuilder, RowType entryType, RemoveFileEntry removeFileEntry)
    {
        pageBuilder.declarePosition();
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(REMOVE_BLOCK_CHANNEL);
        BlockBuilder entryBlockBuilder = blockBuilder.beginBlockEntry();
        writeString(entryBlockBuilder, entryType, 0, "path", removeFileEntry.getPath());
        writeLong(entryBlockBuilder, entryType, 1, "deletionTimestamp", removeFileEntry.getDeletionTimestamp());
        writeBoolean(entryBlockBuilder, entryType, 2, "dataChange", removeFileEntry.isDataChange());
        blockBuilder.closeEntry();

        // null for others
        appendNullOtherBlocks(pageBuilder, REMOVE_BLOCK_CHANNEL);
    }

    private void appendNullOtherBlocks(PageBuilder pageBuilder, int handledBlock)
    {
        for (int channel = 0; channel < CHANNELS_COUNT; ++channel) {
            if (channel == handledBlock) {
                continue;
            }
            pageBuilder.getBlockBuilder(channel).appendNull();
        }
    }

    private void writeString(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, @Nullable String value)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        field.getType().writeSlice(blockBuilder, utf8Slice(value));
    }

    private void writeLong(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, @Nullable Long value)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }
        field.getType().writeLong(blockBuilder, value);
    }

    private void writeBoolean(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, boolean value)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        field.getType().writeBoolean(blockBuilder, value);
    }

    private void writeStringMap(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, @Nullable Map<String, String> values)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        checkArgument(field.getType() instanceof MapType, "Expected field %s/%s to by of MapType but got %s", fieldId, fieldName, field.getType());
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        MapType mapType = (MapType) field.getType();
        BlockBuilder mapBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            mapType.getKeyType().writeSlice(mapBuilder, utf8Slice(entry.getKey()));
            if (entry.getValue() == null) {
                mapBuilder.appendNull();
            }
            else {
                mapType.getKeyType().writeSlice(mapBuilder, utf8Slice(entry.getValue()));
            }
        }
        blockBuilder.closeEntry();
    }

    private void writeStringList(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, @Nullable List<String> values)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        checkArgument(field.getType() instanceof ArrayType, "Expected field %s/%s to by of ArrayType but got %s", fieldId, fieldName, field.getType());
        if (values == null) {
            blockBuilder.appendNull();
            return;
        }
        ArrayType arrayType = (ArrayType) field.getType();
        BlockBuilder mapBuilder = blockBuilder.beginBlockEntry();
        for (String value : values) {
            if (value == null) {
                mapBuilder.appendNull();
            }
            else {
                arrayType.getElementType().writeSlice(mapBuilder, utf8Slice(value));
            }
        }
        blockBuilder.closeEntry();
    }

    private RowType getInternalRowType(RowType type, int fieldId, String fieldName)
    {
        RowType.Field field = validateAndGetField(type, fieldId, fieldName);
        checkArgument(field.getType() instanceof RowType, "Expected field %s/%s to by of RowType but got %s", fieldId, fieldName, field.getType());
        return (RowType) field.getType();
    }

    private RowType.Field validateAndGetField(RowType type, int fieldId, String fieldName)
    {
        checkArgument(type.getFields().size() > fieldId, "Field %s/%s not found for type %s", fieldId, fieldName, type);
        RowType.Field field = type.getFields().get(fieldId);
        // anonymous row fields are not expected here
        checkArgument(field.getName().orElseThrow().equals(fieldName), "Expected %s for field %s but got %s for type %s", fieldName, fieldId, field.getName(), type);
        return field;
    }
}
