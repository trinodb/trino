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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.parquet.writer.ParquetSchemaConverter;
import io.trino.parquet.writer.ParquetWriter;
import io.trino.parquet.writer.ParquetWriterOptions;
import io.trino.plugin.deltalake.DeltaLakeColumnMetadata;
import io.trino.plugin.deltalake.transactionlog.AddFileEntry;
import io.trino.plugin.deltalake.transactionlog.MetadataEntry;
import io.trino.plugin.deltalake.transactionlog.ProtocolEntry;
import io.trino.plugin.deltalake.transactionlog.RemoveFileEntry;
import io.trino.plugin.deltalake.transactionlog.TransactionEntry;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeJsonFileStatistics;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeParquetFileStatistics;
import io.trino.spi.PageBuilder;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.MapBlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.DateTimeEncoding;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.RowType.Field;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import jakarta.annotation.Nullable;
import org.apache.parquet.format.CompressionCodec;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.jsonValueToTrinoValue;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.toJsonValues;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeParquetStatisticsUtils.toNullCounts;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.extractSchema;
import static io.trino.plugin.deltalake.transactionlog.DeltaLakeSchemaSupport.serializeStatsAsJson;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHECKPOINT_WRITE_STATS_AS_JSON_PROPERTY;
import static io.trino.plugin.deltalake.transactionlog.MetadataEntry.DELTA_CHECKPOINT_WRITE_STATS_AS_STRUCT_PROPERTY;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;
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
    private final String trinoVersion;
    private final ParquetWriterOptions parquetWriterOptions;

    public CheckpointWriter(TypeManager typeManager, CheckpointSchemaManager checkpointSchemaManager, String trinoVersion)
    {
        this(typeManager, checkpointSchemaManager, trinoVersion, ParquetWriterOptions.builder().build());
    }

    @VisibleForTesting
    public CheckpointWriter(TypeManager typeManager, CheckpointSchemaManager checkpointSchemaManager, String trinoVersion, ParquetWriterOptions parquetWriterOptions)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.checkpointSchemaManager = requireNonNull(checkpointSchemaManager, "checkpointSchemaManager is null");
        this.trinoVersion = requireNonNull(trinoVersion, "trinoVersion is null");
        this.parquetWriterOptions = requireNonNull(parquetWriterOptions, "parquetWriterOptions is null");
    }

    public void write(CheckpointEntries entries, TrinoOutputFile outputFile)
            throws IOException
    {
        Map<String, String> configuration = entries.getMetadataEntry().getConfiguration();
        boolean writeStatsAsJson = Boolean.parseBoolean(configuration.getOrDefault(DELTA_CHECKPOINT_WRITE_STATS_AS_JSON_PROPERTY, "true"));
        // The default value is false in https://github.com/delta-io/delta/blob/master/PROTOCOL.md#checkpoint-format, but Databricks defaults to true
        boolean writeStatsAsStruct = Boolean.parseBoolean(configuration.getOrDefault(DELTA_CHECKPOINT_WRITE_STATS_AS_STRUCT_PROPERTY, "true"));

        ProtocolEntry protocolEntry = entries.getProtocolEntry();

        RowType metadataEntryType = checkpointSchemaManager.getMetadataEntryType();
        RowType protocolEntryType = checkpointSchemaManager.getProtocolEntryType(protocolEntry.getReaderFeatures().isPresent(), protocolEntry.getWriterFeatures().isPresent());
        RowType txnEntryType = checkpointSchemaManager.getTxnEntryType();
        RowType addEntryType = checkpointSchemaManager.getAddEntryType(entries.getMetadataEntry(), writeStatsAsJson, writeStatsAsStruct);
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

        ParquetSchemaConverter schemaConverter = new ParquetSchemaConverter(columnTypes, columnNames, false, false);

        ParquetWriter writer = new ParquetWriter(
                outputFile.create(),
                schemaConverter.getMessageType(),
                schemaConverter.getPrimitiveTypes(),
                parquetWriterOptions,
                CompressionCodec.SNAPPY,
                trinoVersion,
                Optional.of(DateTimeZone.UTC),
                Optional.empty());

        PageBuilder pageBuilder = new PageBuilder(columnTypes);

        writeMetadataEntry(pageBuilder, metadataEntryType, entries.getMetadataEntry());
        writeProtocolEntry(pageBuilder, protocolEntryType, entries.getProtocolEntry());
        for (TransactionEntry transactionEntry : entries.getTransactionEntries()) {
            writeTransactionEntry(pageBuilder, txnEntryType, transactionEntry);
        }
        for (AddFileEntry addFileEntry : entries.getAddFileEntries()) {
            writeAddFileEntry(pageBuilder, addEntryType, addFileEntry, entries.getMetadataEntry(), writeStatsAsJson, writeStatsAsStruct);
        }
        for (RemoveFileEntry removeFileEntry : entries.getRemoveFileEntries()) {
            writeRemoveFileEntry(pageBuilder, removeEntryType, removeFileEntry);
        }
        // Not writing commit infos for now. DB does not keep them in the checkpoints by default

        writer.write(pageBuilder.build());
        writer.close();
    }

    private void writeMetadataEntry(PageBuilder pageBuilder, RowType entryType, MetadataEntry metadataEntry)
    {
        pageBuilder.declarePosition();
        ((RowBlockBuilder) pageBuilder.getBlockBuilder(METADATA_BLOCK_CHANNEL)).buildEntry(fieldBuilders -> {
            writeString(fieldBuilders.get(0), entryType, 0, "id", metadataEntry.getId());
            writeString(fieldBuilders.get(1), entryType, 1, "name", metadataEntry.getName());
            writeString(fieldBuilders.get(2), entryType, 2, "description", metadataEntry.getDescription());

            RowType formatType = getInternalRowType(entryType, 3, "format");
            ((RowBlockBuilder) fieldBuilders.get(3)).buildEntry(formatBlockBuilders -> {
                writeString(formatBlockBuilders.get(0), formatType, 0, "provider", metadataEntry.getFormat().getProvider());
                writeStringMap(formatBlockBuilders.get(1), formatType, 1, "options", metadataEntry.getFormat().getOptions());
            });

            writeString(fieldBuilders.get(4), entryType, 4, "schemaString", metadataEntry.getSchemaString());
            writeStringList(fieldBuilders.get(5), entryType, 5, "partitionColumns", metadataEntry.getOriginalPartitionColumns());
            writeStringMap(fieldBuilders.get(6), entryType, 6, "configuration", metadataEntry.getConfiguration());
            writeLong(fieldBuilders.get(7), entryType, 7, "createdTime", metadataEntry.getCreatedTime());
        });

        // null for others
        appendNullOtherBlocks(pageBuilder, METADATA_BLOCK_CHANNEL);
    }

    private void writeProtocolEntry(PageBuilder pageBuilder, RowType entryType, ProtocolEntry protocolEntry)
    {
        pageBuilder.declarePosition();
        ((RowBlockBuilder) pageBuilder.getBlockBuilder(PROTOCOL_BLOCK_CHANNEL)).buildEntry(fieldBuilders -> {
            int fieldId = 0;
            writeLong(fieldBuilders.get(fieldId), entryType, fieldId, "minReaderVersion", (long) protocolEntry.getMinReaderVersion());
            fieldId++;

            writeLong(fieldBuilders.get(fieldId), entryType, fieldId, "minWriterVersion", (long) protocolEntry.getMinWriterVersion());
            fieldId++;

            if (protocolEntry.getReaderFeatures().isPresent()) {
                writeStringList(fieldBuilders.get(fieldId), entryType, fieldId, "readerFeatures", protocolEntry.getReaderFeatures().get().stream().collect(toImmutableList()));
                fieldId++;
            }

            if (protocolEntry.getWriterFeatures().isPresent()) {
                writeStringList(fieldBuilders.get(fieldId), entryType, fieldId, "writerFeatures", protocolEntry.getWriterFeatures().get().stream().collect(toImmutableList()));
            }
        });

        // null for others
        appendNullOtherBlocks(pageBuilder, PROTOCOL_BLOCK_CHANNEL);
    }

    private void writeTransactionEntry(PageBuilder pageBuilder, RowType entryType, TransactionEntry transactionEntry)
    {
        pageBuilder.declarePosition();
        ((RowBlockBuilder) pageBuilder.getBlockBuilder(TXN_BLOCK_CHANNEL)).buildEntry(fieldBuilders -> {
            writeString(fieldBuilders.get(0), entryType, 0, "appId", transactionEntry.getAppId());
            writeLong(fieldBuilders.get(1), entryType, 1, "version", transactionEntry.getVersion());
            writeLong(fieldBuilders.get(2), entryType, 2, "lastUpdated", transactionEntry.getLastUpdated());
        });

        // null for others
        appendNullOtherBlocks(pageBuilder, TXN_BLOCK_CHANNEL);
    }

    private void writeAddFileEntry(PageBuilder pageBuilder, RowType entryType, AddFileEntry addFileEntry, MetadataEntry metadataEntry, boolean writeStatsAsJson, boolean writeStatsAsStruct)
    {
        pageBuilder.declarePosition();
        RowBlockBuilder blockBuilder = (RowBlockBuilder) pageBuilder.getBlockBuilder(ADD_BLOCK_CHANNEL);
        blockBuilder.buildEntry(fieldBuilders -> {
            int fieldId = 0;
            writeString(fieldBuilders.get(fieldId), entryType, fieldId, "path", addFileEntry.getPath());
            fieldId++;

            writeStringMap(fieldBuilders.get(fieldId), entryType, fieldId, "partitionValues", addFileEntry.getPartitionValues());
            fieldId++;

            writeLong(fieldBuilders.get(fieldId), entryType, fieldId, "size", addFileEntry.getSize());
            fieldId++;

            writeLong(fieldBuilders.get(fieldId), entryType, fieldId, "modificationTime", addFileEntry.getModificationTime());
            fieldId++;

            writeBoolean(fieldBuilders.get(fieldId), entryType, fieldId, "dataChange", addFileEntry.isDataChange());
            fieldId++;

            if (writeStatsAsJson) {
                writeJsonStats(fieldBuilders.get(fieldId), entryType, addFileEntry, metadataEntry, fieldId);
                fieldId++;
            }

            if (writeStatsAsStruct) {
                writeParsedStats(fieldBuilders.get(fieldId), entryType, addFileEntry, fieldId);
                fieldId++;
            }

            writeStringMap(fieldBuilders.get(fieldId), entryType, fieldId, "tags", addFileEntry.getTags());
        });

        // null for others
        appendNullOtherBlocks(pageBuilder, ADD_BLOCK_CHANNEL);
    }

    private void writeJsonStats(BlockBuilder entryBlockBuilder, RowType entryType, AddFileEntry addFileEntry, MetadataEntry metadataEntry, int fieldId)
    {
        String statsJson = null;
        if (addFileEntry.getStats().isPresent()) {
            DeltaLakeFileStatistics statistics = addFileEntry.getStats().get();
            if (statistics instanceof DeltaLakeParquetFileStatistics parquetFileStatistics) {
                Map<String, Type> columnTypeMapping = getColumnTypeMapping(metadataEntry);
                DeltaLakeJsonFileStatistics jsonFileStatistics = new DeltaLakeJsonFileStatistics(
                        parquetFileStatistics.getNumRecords(),
                        parquetFileStatistics.getMinValues().map(values -> toJsonValues(columnTypeMapping, values)),
                        parquetFileStatistics.getMaxValues().map(values -> toJsonValues(columnTypeMapping, values)),
                        parquetFileStatistics.getNullCount().map(nullCounts -> toNullCounts(columnTypeMapping, nullCounts)));
                statsJson = getStatsString(jsonFileStatistics).orElse(null);
            }
            else {
                statsJson = addFileEntry.getStatsString().orElse(null);
            }
        }
        writeString(entryBlockBuilder, entryType, fieldId, "stats", statsJson);
    }

    private Map<String, Type> getColumnTypeMapping(MetadataEntry deltaMetadata)
    {
        return extractSchema(deltaMetadata, typeManager).stream()
                .collect(toImmutableMap(DeltaLakeColumnMetadata::getPhysicalName, DeltaLakeColumnMetadata::getPhysicalColumnType));
    }

    private Optional<String> getStatsString(DeltaLakeJsonFileStatistics parsedStats)
    {
        try {
            return Optional.of(serializeStatsAsJson(parsedStats));
        }
        catch (JsonProcessingException e) {
            return Optional.empty();
        }
    }

    private void writeParsedStats(BlockBuilder entryBlockBuilder, RowType entryType, AddFileEntry addFileEntry, int fieldId)
    {
        RowType statsType = getInternalRowType(entryType, fieldId, "stats_parsed");
        if (addFileEntry.getStats().isEmpty()) {
            entryBlockBuilder.appendNull();
            return;
        }
        DeltaLakeFileStatistics stats = addFileEntry.getStats().get();
        ((RowBlockBuilder) entryBlockBuilder).buildEntry(fieldBuilders -> {
            if (stats instanceof DeltaLakeParquetFileStatistics) {
                writeLong(fieldBuilders.get(0), statsType, 0, "numRecords", stats.getNumRecords().orElse(null));
                writeMinMaxMapAsFields(fieldBuilders.get(1), statsType, 1, "minValues", stats.getMinValues(), false);
                writeMinMaxMapAsFields(fieldBuilders.get(2), statsType, 2, "maxValues", stats.getMaxValues(), false);
                writeNullCountAsFields(fieldBuilders.get(3), statsType, 3, "nullCount", stats.getNullCount());
            }
            else {
                int internalFieldId = 0;

                writeLong(fieldBuilders.get(internalFieldId), statsType, internalFieldId, "numRecords", stats.getNumRecords().orElse(null));
                internalFieldId++;

                if (statsType.getFields().stream().anyMatch(field -> field.getName().orElseThrow().equals("minValues"))) {
                    writeMinMaxMapAsFields(fieldBuilders.get(internalFieldId), statsType, internalFieldId, "minValues", stats.getMinValues(), true);
                    internalFieldId++;
                }
                if (statsType.getFields().stream().anyMatch(field -> field.getName().orElseThrow().equals("maxValues"))) {
                    writeMinMaxMapAsFields(fieldBuilders.get(internalFieldId), statsType, internalFieldId, "maxValues", stats.getMaxValues(), true);
                    internalFieldId++;
                }
                writeNullCountAsFields(fieldBuilders.get(internalFieldId), statsType, internalFieldId, "nullCount", stats.getNullCount());
            }
        });
    }

    private void writeMinMaxMapAsFields(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, Optional<Map<String, Object>> values, boolean isJson)
    {
        RowType.Field valuesField = validateAndGetField(type, fieldId, fieldName);
        RowType valuesFieldType = (RowType) valuesField.getType();
        writeObjectMapAsFields(blockBuilder, type, fieldId, fieldName, preprocessMinMaxValues(valuesFieldType, values, isJson));
    }

    private void writeNullCountAsFields(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, Optional<Map<String, Object>> values)
    {
        writeObjectMapAsFields(blockBuilder, type, fieldId, fieldName, preprocessNullCount(values));
    }

    private void writeObjectMapAsFields(BlockBuilder blockBuilder, RowType type, int fieldId, String fieldName, Optional<Map<String, Object>> values)
    {
        if (values.isEmpty()) {
            blockBuilder.appendNull();
            return;
        }

        Field valuesField = validateAndGetField(type, fieldId, fieldName);
        List<Field> fields = ((RowType) valuesField.getType()).getFields();
        ((RowBlockBuilder) blockBuilder).buildEntry(fieldBuilders -> {
            for (int i = 0; i < fields.size(); i++) {
                Field field = fields.get(i);
                BlockBuilder fieldBlockBuilder = fieldBuilders.get(i);

                // anonymous row fields are not expected here
                Object value = values.get().get(field.getName().orElseThrow());
                if (field.getType() instanceof RowType) {
                    Block rowBlock = (Block) value;
                    // Statistics were not collected
                    if (rowBlock == null) {
                        fieldBlockBuilder.appendNull();
                        continue;
                    }
                    checkState(rowBlock.getPositionCount() == 1, "Invalid RowType statistics for writing Delta Lake checkpoint");
                    if (rowBlock.isNull(0)) {
                        fieldBlockBuilder.appendNull();
                    }
                    else {
                        field.getType().appendTo(rowBlock, 0, fieldBlockBuilder);
                    }
                }
                else {
                    writeNativeValue(field.getType(), fieldBlockBuilder, value);
                }
            }
        });
    }

    private Optional<Map<String, Object>> preprocessMinMaxValues(RowType valuesType, Optional<Map<String, Object>> valuesOptional, boolean isJson)
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
                                        if (isJson) {
                                            return jsonValueToTrinoValue(type, value);
                                        }
                                        if (type instanceof TimestampType) {
                                            // We need to remap TIMESTAMP WITH TIME ZONE -> TIMESTAMP here because of
                                            // inconsistency in what type is used for DL "timestamp" type in data processing and in min/max statistics map.
                                            value = multiplyExact(DateTimeEncoding.unpackMillisUtc((long) value), MICROSECONDS_PER_MILLISECOND);
                                        }
                                        return value;
                                    }));
                });
    }

    private Optional<Map<String, Object>> preprocessNullCount(Optional<Map<String, Object>> valuesOptional)
    {
        return valuesOptional.map(
                values ->
                        values.entrySet().stream()
                                .collect(toMap(
                                        Map.Entry::getKey,
                                        entry -> {
                                            Object value = entry.getValue();
                                            if (value instanceof Integer) {
                                                return (long) (int) value;
                                            }
                                            return value;
                                        })));
    }

    private void writeRemoveFileEntry(PageBuilder pageBuilder, RowType entryType, RemoveFileEntry removeFileEntry)
    {
        pageBuilder.declarePosition();
        ((RowBlockBuilder) pageBuilder.getBlockBuilder(REMOVE_BLOCK_CHANNEL)).buildEntry(fieldBuilders -> {
            writeString(fieldBuilders.get(0), entryType, 0, "path", removeFileEntry.getPath());
            writeLong(fieldBuilders.get(1), entryType, 1, "deletionTimestamp", removeFileEntry.getDeletionTimestamp());
            writeBoolean(fieldBuilders.get(2), entryType, 2, "dataChange", removeFileEntry.isDataChange());
        });

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
        ((MapBlockBuilder) blockBuilder).buildEntry((keyBlockBuilder, valueBlockBuilder) -> {
            for (Map.Entry<String, String> entry : values.entrySet()) {
                mapType.getKeyType().writeSlice(keyBlockBuilder, utf8Slice(entry.getKey()));
                if (entry.getValue() == null) {
                    valueBlockBuilder.appendNull();
                }
                else {
                    mapType.getValueType().writeSlice(valueBlockBuilder, utf8Slice(entry.getValue()));
                }
            }
        });
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
        ((ArrayBlockBuilder) blockBuilder).buildEntry(elementBuilder -> {
            for (String value : values) {
                if (value == null) {
                    elementBuilder.appendNull();
                }
                else {
                    arrayType.getElementType().writeSlice(elementBuilder, utf8Slice(value));
                }
            }
        });
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
