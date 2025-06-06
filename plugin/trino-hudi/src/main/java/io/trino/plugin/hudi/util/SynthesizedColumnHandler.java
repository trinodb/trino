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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.type.BigintType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DateType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.SqlDecimal;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.metastore.Partitions.makePartName;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_MODIFIED_TIME_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.FILE_SIZE_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PARTITION_COLUMN_NAME;
import static io.trino.plugin.hive.HiveColumnHandle.PATH_COLUMN_NAME;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.Decimals.writeBigDecimal;
import static io.trino.spi.type.Decimals.writeShortDecimal;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;

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
        builder.put(PARTITION_COLUMN_NAME, (blockBuilder, _) ->
                VarcharType.VARCHAR.writeSlice(blockBuilder,
                        utf8Slice(toPartitionName(splitMetadata.getPartitionKeyVals()))));

        builder.put(PATH_COLUMN_NAME, (blockBuilder, _) ->
                VarcharType.VARCHAR.writeSlice(blockBuilder, utf8Slice(splitMetadata.getFilePath())));

        builder.put(FILE_SIZE_COLUMN_NAME, (blockBuilder, _) ->
                BigintType.BIGINT.writeLong(blockBuilder, splitMetadata.getFileSize()));

        builder.put(FILE_MODIFIED_TIME_COLUMN_NAME, (blockBuilder, _) -> {
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
        // Type is ignored here as input partitionKey.value() is always passed as a String type
        for (HivePartitionKey partitionKey : hudiSplit.getPartitionKeys()) {
            builder.put(partitionKey.name(), (blockBuilder, targetType) ->
                    appendPartitionKey(targetType, partitionKey.value(), blockBuilder));
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
     * Retrieves the count of synthesized column strategies currently present.
     *
     * @return The number of synthesized column strategies.
     */
    public int getSynthesizedColumnCount()
    {
        return strategies.size();
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
     * Creates a {@link Block} for the given synthesized column, typically a {@link RunLengthEncodedBlock} as the synthesized value is constant for all positions within a split.
     *
     * @param columnHandle The handle of the synthesized column to create a block for.
     * @param positionCount The number of positions (rows) the resulting block should represent.
     * @return A {@link Block} containing the synthesized values.
     */
    public Block createRleSynthesizedBlock(HiveColumnHandle columnHandle, int positionCount)
    {
        Type columnType = columnHandle.getType();

        if (positionCount == 0) {
            return columnType.createBlockBuilder(null, 0).build();
        }

        SynthesizedColumnStrategy strategy = getColumnStrategy(columnHandle);

        // Because this builder will only hold the single constant value
        int expectedEntriesForValueBlock = 1;
        BlockBuilder valueBuilder = columnType.createBlockBuilder(null, expectedEntriesForValueBlock);

        if (strategy == null) {
            valueBuilder.appendNull();
        }
        else {
            // Apply the strategy to write the single value into the builder
            strategy.appendToBlock(valueBuilder, columnType);
        }
        Block valueBlock = valueBuilder.build();

        return RunLengthEncodedBlock.create(valueBlock, positionCount);
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

        /**
         * Creates SplitMetadata from a Hudi split and partition key list.
         */
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

    /**
     * Helper function to prefill BlockBuilders with values from PartitionKeys which are in the String type.
     * This function handles the casting of String type the actual column type.
     */
    private static void appendPartitionKey(Type targetType, String value, BlockBuilder blockBuilder)
    {
        if (value == null) {
            blockBuilder.appendNull();
            return;
        }

        if (targetType instanceof VarcharType varcharType) {
            varcharType.writeSlice(blockBuilder, utf8Slice(value));
        }
        else if (targetType instanceof IntegerType integerType) {
            integerType.writeInt(blockBuilder, Integer.parseInt(value));
        }
        else if (targetType instanceof BigintType bigintType) {
            bigintType.writeLong(blockBuilder, Long.parseLong(value));
        }
        else if (targetType instanceof BooleanType booleanType) {
            booleanType.writeBoolean(blockBuilder, Boolean.parseBoolean(value));
        }
        else if (targetType instanceof DecimalType decimalType) {
            SqlDecimal sqlDecimal = SqlDecimal.decimal(value, decimalType);
            BigDecimal bigDecimal = sqlDecimal.toBigDecimal();

            if (decimalType.isShort()) {
                // For short decimals, get the unscaled long value
                // SqlDecimal.toBigDecimal() is used for consistency with the original SqlDecimal path
                // The unscaled value of a Trino short decimal (precision <= 18) fits in a long
                writeShortDecimal(blockBuilder, bigDecimal.unscaledValue().longValue());
            }
            else {
                // For long decimals, use the BigDecimal representation obtained from SqlDecimal.
                writeBigDecimal(decimalType, blockBuilder, bigDecimal);
            }
        }
        else if (targetType instanceof DateType dateType) {
            try {
                // Parse the date string using "YYYY-MM-DD" format
                LocalDate localDate = LocalDate.parse(value);
                // Convert LocalDate to days since epoch where LocalDate#toEpochDay() returns a long
                int daysSinceEpoch = toIntExact(localDate.toEpochDay());
                dateType.writeInt(blockBuilder, daysSinceEpoch);
            }
            catch (DateTimeParseException e) {
                // Handle parsing error
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Invalid date string format for DATE type: '%s'. Expected format like YYYY-MM-DD. Details: %s",
                                value, e.getMessage()), e);
            }
            catch (ArithmeticException e) {
                // Handle potential overflow if toEpochDay() result is outside int range
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Date string '%s' results in a day count out of integer range for DATE type. Details: %s",
                                value, e.getMessage()), e);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR, format("Unknown target type '%s' for column '%s'", targetType, value));
        }
    }
}
