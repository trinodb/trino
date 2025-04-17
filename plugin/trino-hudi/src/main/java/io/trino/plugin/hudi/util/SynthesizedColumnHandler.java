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
package io.trino.plugin.hudi.util;

import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiFile;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.VarcharType;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;

/**
 * Handles synthesized (virtual) columns in Hudi tables, such as partition columns and metadata (not hudi metadata)
 * columns.
 */
public class SynthesizedColumnHandler
{
    private final Map<String, SynthesizedColumnStrategy> strategies;
    private final SplitMetadata splitMetadata;

    public static SynthesizedColumnHandler create(HudiSplit hudiSplit)
    {
        return new SynthesizedColumnHandler(hudiSplit);
    }

    /**
     * Constructs a SynthesizedColumnHandler with the given partition keys.
     */
    public SynthesizedColumnHandler(HudiSplit hudiSplit)
    {
        this.splitMetadata = SplitMetadata.of(hudiSplit);
        ImmutableMap.Builder<String, SynthesizedColumnStrategy> builder = ImmutableMap.builder();
        initSynthesizedColStrategies(builder);
        initPartitionKeyStrategies(builder, hudiSplit);
        strategies = builder.buildOrThrow();
    }

    /**
     * Initializes strategies for synthesized columns.
     */
    private void initSynthesizedColStrategies(ImmutableMap.Builder<String, SynthesizedColumnStrategy> builder)
    {
        builder.put(PARTITION_COLUMN_NAME, blockBuilder ->
                VarcharType.VARCHAR.writeSlice(blockBuilder,
                        utf8Slice(toPartitionName(splitMetadata.getPartitionKeyVals()))));

        builder.put(PATH_COLUMN_NAME, blockBuilder ->
                VarcharType.VARCHAR.writeSlice(blockBuilder, utf8Slice(splitMetadata.getFilePath())));

        builder.put(FILE_SIZE_COLUMN_NAME, blockBuilder ->
                BigintType.BIGINT.writeLong(blockBuilder, splitMetadata.getFileSize()));

        builder.put(FILE_MODIFIED_TIME_COLUMN_NAME, blockBuilder -> {
            long packedTimestamp = packDateTimeWithZone(
                    splitMetadata.getFileModificationTime(), UTC_KEY);
            TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, packedTimestamp);
        });
    }

    /**
     * Initializes strategies for partition columns.
     */
    private void initPartitionKeyStrategies(ImmutableMap.Builder<String, SynthesizedColumnStrategy> builder,
            HudiSplit hudiSplit)
    {
        for (HivePartitionKey partitionKey : hudiSplit.getPartitionKeys()) {
            builder.put(partitionKey.name(), (blockBuilder) ->
                    VarcharType.VARCHAR.writeSlice(blockBuilder, utf8Slice(partitionKey.value())));
        }
    }

    /**
     * Checks if a column is a synthesized column.
     *
     * @param columnName The column name.
     * @return True if the column is synthesized, false otherwise.
     */
    public boolean isSynthesizedColumn(String columnName)
    {
        return strategies.containsKey(columnName);
    }

    /**
     * Checks if a Hive column handle represents a synthesized column.
     *
     * @param columnHandle The Hive column handle.
     * @return True if the column is synthesized, false otherwise.
     */
    public boolean isSynthesizedColumn(HiveColumnHandle columnHandle)
    {
        return isSynthesizedColumn(columnHandle.getName());
    }

    /**
     * Retrieves the strategy for a given synthesized column.
     *
     * @param columnHandle The Hive column handle.
     * @return The corresponding column strategy, or null if not found.
     */
    public SynthesizedColumnStrategy getColumnStrategy(HiveColumnHandle columnHandle)
    {
        return strategies.get(columnHandle.getName());
    }

    /**
     * Converts partition key-value pairs into a partition name string.
     *
     * @param partitionKeyVals Map of partition key-value pairs.
     * @return Partition name string.
     */
    private static String toPartitionName(Map<String, String> partitionKeyVals)
    {
        return makePartName(List.copyOf(partitionKeyVals.keySet()), List.copyOf(partitionKeyVals.values()));
    }

    /**
     * Represents metadata about split being processed.
     * Splits are assumed to be in the same partition.
     */
    public static class SplitMetadata
    {
        private final Map<String, String> partitionKeyVals;
        private final String filePath;
        private final long fileSize;
        private final long modifiedTime;

        public static SplitMetadata of(HudiSplit hudiSplit)
        {
            return new SplitMetadata(hudiSplit);
        }

        public SplitMetadata(HudiSplit hudiSplit)
        {
            this.partitionKeyVals = hudiSplit.getPartitionKeys().stream()
                    .collect(Collectors.toMap(HivePartitionKey::name, HivePartitionKey::value));
            // Parquet files will be prioritised over log files
            HudiFile hudiFile = hudiSplit.getBaseFile().isPresent()
                    ? hudiSplit.getBaseFile().get()
                    : hudiSplit.getLogFiles().getFirst();
            this.filePath = hudiFile.getPath();
            this.fileSize = hudiFile.getFileSize();
            this.modifiedTime = hudiFile.getModificationTime();
        }

        public Map<String, String> getPartitionKeyVals()
        {
            return partitionKeyVals;
        }

        public String getFilePath()
        {
            return filePath;
        }

        public long getFileSize()
        {
            return fileSize;
        }

        public long getFileModificationTime()
        {
            return modifiedTime;
        }
    }
}
