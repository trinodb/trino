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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.TrinoInputFile;
import io.trino.plugin.iceberg.util.PageListBuilder;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnNotFoundException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FilesMetadataTable;
import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.transforms.Transforms;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.StructLikeWrapper;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.plugin.iceberg.IcebergTypes.convertIcebergValueToTrino;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static org.apache.iceberg.FilesMetadataTable.COLUMN_SIZES;
import static org.apache.iceberg.FilesMetadataTable.CONTENT;
import static org.apache.iceberg.FilesMetadataTable.EQUALITY_IDS;
import static org.apache.iceberg.FilesMetadataTable.FILE_FORMAT;
import static org.apache.iceberg.FilesMetadataTable.FILE_SIZE;
import static org.apache.iceberg.FilesMetadataTable.HIDDEN_FILE_PATH;
import static org.apache.iceberg.FilesMetadataTable.HIDDEN_MODIFIED_FILE_PATH;
import static org.apache.iceberg.FilesMetadataTable.KEY_METADATA;
import static org.apache.iceberg.FilesMetadataTable.LOWER_BOUNDS;
import static org.apache.iceberg.FilesMetadataTable.NAN_VALUE_COUNTS;
import static org.apache.iceberg.FilesMetadataTable.NULL_VALUE_COUNTS;
import static org.apache.iceberg.FilesMetadataTable.PARTITION_ID;
import static org.apache.iceberg.FilesMetadataTable.RECORD_COUNT;
import static org.apache.iceberg.FilesMetadataTable.SORT_ORDER_ID;
import static org.apache.iceberg.FilesMetadataTable.SPEC_ID;
import static org.apache.iceberg.FilesMetadataTable.SPLIT_OFFSETS;
import static org.apache.iceberg.FilesMetadataTable.UPPER_BOUNDS;
import static org.apache.iceberg.FilesMetadataTable.VALUE_COUNTS;

public class ManifestPageSource
        implements ConnectorPageSource
{
    private final int specId;
    private SchemaTableName schemaTableName;
    private final Schema baseTableSchema;
    private final Optional<Map<Integer, PartitionSpec>> basePartitionSpecs;
    private final long length;
    private final FileIO fileIO;
    private final List<Type> columnTypes;
    private final List<IcebergColumnHandle> columnHandles;
    private long readTimeNanos;
    private Iterator<Page> pages;
    private final Map<Integer, org.apache.iceberg.types.Type> idToTypeMapping;
    private final TrinoInputFile inputFile;

    private final PartitionSpec baseTablePartitionSpec;

    public ManifestPageSource(
            SchemaTableName schemaTableName,
            Schema baseTableSchema,
            PartitionSpec baseTablePartitionSpec,
            Optional<Map<Integer, PartitionSpec>> basePartitionSpecs,
            long length,
            List<IcebergColumnHandle> columnHandles,
            int partitionSpecId,
            FileIO fileIO,
            TrinoInputFile inputFile)
    {
        this.schemaTableName = schemaTableName;
        this.baseTableSchema = baseTableSchema;
        this.basePartitionSpecs = basePartitionSpecs;
        this.baseTablePartitionSpec = baseTablePartitionSpec;
        this.length = length;
        this.specId = partitionSpecId;
        this.columnHandles = columnHandles;
        this.columnTypes = columnHandles.stream().map(ch -> ch.getType()).collect(Collectors.toList());
        this.fileIO = fileIO;
        this.idToTypeMapping = getIcebergIdToTypeMapping(baseTableSchema);
        this.inputFile = inputFile;
    }

    private static Map<Integer, org.apache.iceberg.types.Type> getIcebergIdToTypeMapping(Schema schema)
    {
        ImmutableMap.Builder<Integer, org.apache.iceberg.types.Type> icebergIdToTypeMapping = ImmutableMap.builder();
        for (Types.NestedField field : schema.columns()) {
            populateIcebergIdToTypeMapping(field, icebergIdToTypeMapping);
        }
        return icebergIdToTypeMapping.buildOrThrow();
    }

    private static void populateIcebergIdToTypeMapping(Types.NestedField field, ImmutableMap.Builder<Integer, org.apache.iceberg.types.Type> icebergIdToTypeMapping)
    {
        org.apache.iceberg.types.Type type = field.type();
        icebergIdToTypeMapping.put(field.fieldId(), type);
        if (type instanceof org.apache.iceberg.types.Type.NestedType) {
            type.asNestedType().fields().forEach(child -> populateIcebergIdToTypeMapping(child, icebergIdToTypeMapping));
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return pages != null && !pages.hasNext();
    }

    @Override
    public Page getNextPage()
    {
        long start = System.nanoTime();
        PageListBuilder pageListBuilder = new PageListBuilder(columnTypes);

        // THE FIRST READ IS NECESSARY AS THAT GIVES US THE ACTUAL SPECS. THIS IS POTENTIALLY A BUG IN ICEBERG MEATADATA SCAN READ WHICH ASSUMES
        // THAT ANYTHING READING THE SCANFILE OUTPUT WOULD JUST USE DATATASK AND RELY ON IT TO READ RECORDS.
        GenericManifestFile manifestFile = new GenericManifestFile(inputFile.location().toString(), length, specId, ManifestContent.DATA, -1, -1, 1L, 0, 0, 0, 0, 0, 0, null, null);
        PartitionSpec spec = ManifestFiles.read(manifestFile, fileIO).spec();

        manifestFile = new GenericManifestFile(inputFile.location().toString(), length, spec.specId(), ManifestContent.DATA, -1, -1, 1L, 0, 0, 0, 0, 0, 0, null, null);
        CloseableIterator<DataFile> dataFileIterator = ManifestFiles.read(manifestFile, fileIO).iterator();
        while (dataFileIterator.hasNext()) {
            DataFile dataFile = dataFileIterator.next();
            pageListBuilder.beginRow();
            for (IcebergColumnHandle columnHandle : columnHandles) {
                switch (columnHandle.getId()) {
                    case CONTENT:
                        pageListBuilder.appendInteger(dataFile.content().id());
                        break;
                    case FilesMetadataTable.FILE_PATH:
                        pageListBuilder.appendVarchar(dataFile.path().toString());
                        break;
                    case FILE_FORMAT:
                        pageListBuilder.appendVarchar(dataFile.format().name());
                        break;
                    case SPEC_ID:
                        pageListBuilder.appendInteger(dataFile.specId());
                        break;
                    case PARTITION_ID:
                        BlockBuilder partitionBlockBuilder = pageListBuilder.nextColumn();
                        BlockBuilder partitionRowBlockEntry = partitionBlockBuilder.beginBlockEntry();
                        StructLike partitionStruct = dataFile.partition();
                        Types.StructType structType = spec.partitionType();
                        StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(structType).set(partitionStruct);
                        PartitionTable.StructLikeWrapperWithFieldIdToIndex structLikeWrapperWithFieldIdToIndex = new PartitionTable.StructLikeWrapperWithFieldIdToIndex(partitionWrapper, structType);
                        RowType partitionColumnType = (RowType) columnHandle.getType();

                        List<io.trino.spi.type.Type> partitionColumnTypes = partitionColumnType.getFields().stream()
                                .map(RowType.Field::getType)
                                .collect(toImmutableList());
                        for (int i = 0; i < partitionColumnTypes.size(); i++) {
                            io.trino.spi.type.Type trinoType = partitionColumnType.getFields().get(i).getType();
                            Object value = null;
                            Integer fieldId = baseTablePartitionSpec.fields().get(i).fieldId();
                            if (structLikeWrapperWithFieldIdToIndex.fieldIdToIndex.containsKey(fieldId)) {
                                value = convertIcebergValueToTrino(
                                        baseTablePartitionSpec.partitionType().fields().get(i).type(),
                                        structLikeWrapperWithFieldIdToIndex.structLikeWrapper.get().get(structLikeWrapperWithFieldIdToIndex.fieldIdToIndex.get(fieldId), baseTablePartitionSpec.partitionType().fields().get(i).type().typeId().javaClass()));
                            }
                            writeNativeValue(trinoType, partitionRowBlockEntry, value);
                        }
                        partitionBlockBuilder.closeEntry();
                        break;
                    case RECORD_COUNT:
                        pageListBuilder.appendBigint(dataFile.recordCount());
                        break;
                    case FILE_SIZE:
                        pageListBuilder.appendBigint(dataFile.fileSizeInBytes());
                        break;
                    case COLUMN_SIZES:
                        pageListBuilder.appendIntegerBigintMap(dataFile.columnSizes());
                        break;
                    case VALUE_COUNTS:
                        pageListBuilder.appendIntegerBigintMap(dataFile.valueCounts());
                        break;
                    case NULL_VALUE_COUNTS:
                        pageListBuilder.appendIntegerBigintMap(dataFile.nullValueCounts());
                        break;
                    case NAN_VALUE_COUNTS:
                        pageListBuilder.appendIntegerBigintMap(dataFile.nanValueCounts());
                        break;
                    case LOWER_BOUNDS:
                        pageListBuilder.appendVarcharVarcharMap(getStringVarcharMap(dataFile.lowerBounds()));
                        break;
                    case UPPER_BOUNDS:
                        pageListBuilder.appendVarcharVarcharMap(getStringVarcharMap(dataFile.upperBounds()));
                        break;
                    case KEY_METADATA:
                        pageListBuilder.appendVarbinary(dataFile.keyMetadata());
                        break;
                    case SPLIT_OFFSETS:
                        pageListBuilder.appendBigintArray(dataFile.splitOffsets());
                        break;
                    case EQUALITY_IDS:
                        pageListBuilder.appendIntegerArray(dataFile.equalityFieldIds());
                        break;
                    case SORT_ORDER_ID:
                        pageListBuilder.appendInteger(dataFile.sortOrderId());
                        break;
                    case HIDDEN_FILE_PATH:
                        pageListBuilder.appendVarchar(inputFile.location().toString());
                        break;
                    case HIDDEN_MODIFIED_FILE_PATH:
                        try {
                            pageListBuilder.appendTimestampTzMillis(inputFile.lastModified().toEpochMilli(), UTC_KEY);
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                        break;
                    default:
                        throw new ColumnNotFoundException(schemaTableName, columnHandle.getName());
                }
            }
            pageListBuilder.endRow();
        }

        this.readTimeNanos += System.nanoTime() - start;
        this.pages = pageListBuilder.build().iterator();
        return isFinished() ? null : pages.next();
    }

    private Map<String, String> getStringVarcharMap(Map<Integer, ByteBuffer> value)
    {
        if (value == null) {
            return null;
        }
        return value.entrySet().stream()
                .filter(entry -> idToTypeMapping.containsKey(entry.getKey()))
                .collect(toImmutableMap(
                        entry -> baseTableSchema.findField(entry.getKey()).name(),
                        entry -> Transforms.identity().toHumanString(idToTypeMapping.get(entry.getKey()), Conversions.fromByteBuffer(idToTypeMapping.get(entry.getKey()), entry.getValue()))));
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
    }
}
