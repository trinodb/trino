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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hudi.HudiSplit;
import io.trino.plugin.hudi.file.HudiFile;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
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
 * Utility methods for producing constant (RLE) {@link Block}s for synthesized columns
 * (partition keys and virtual hidden columns such as {@code $path}, {@code $file_size},
 * {@code $file_modified_time}, {@code $partition}) in a Hudi split.
 */
public final class HudiSplitColumns
{
    private HudiSplitColumns() {}

    /**
     * Creates a single-value {@link Block} for a synthesized column. The caller
     * (e.g. {@link io.trino.plugin.hive.TransformConnectorPageSource}) wraps it in a
     * {@link io.trino.spi.block.RunLengthEncodedBlock} to replicate it across all rows.
     */
    public static Block createConstantBlock(HudiSplit split, HiveColumnHandle column)
    {
        HudiFile file = split.getBaseFile().isPresent()
                ? split.getBaseFile().get()
                : split.getLogFiles().getFirst();
        List<HivePartitionKey> partitionKeys = split.getPartitionKeys();

        Type type = column.getType();
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        switch (column.getName()) {
            case PATH_COLUMN_NAME ->
                    VarcharType.VARCHAR.writeSlice(builder, utf8Slice(file.getPath()));
            case FILE_SIZE_COLUMN_NAME ->
                    BigintType.BIGINT.writeLong(builder, file.getFileSize());
            case FILE_MODIFIED_TIME_COLUMN_NAME ->
                    TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS.writeLong(builder,
                            packDateTimeWithZone(file.getModificationTime(), UTC_KEY));
            case PARTITION_COLUMN_NAME ->
                    VarcharType.VARCHAR.writeSlice(builder, utf8Slice(makePartName(
                            partitionKeys.stream().map(HivePartitionKey::name).toList(),
                            partitionKeys.stream().map(HivePartitionKey::value).toList())));
            default -> {
                // Partition key column — look up the string value by name and cast to target type
                String value = partitionKeys.stream()
                        .filter(pk -> pk.name().equals(column.getName()))
                        .map(HivePartitionKey::value)
                        .findFirst()
                        .orElse(null);
                appendPartitionKey(type, value, builder);
            }
        }
        return builder.build();
    }

    /**
     * Writes a partition key string value into a {@link BlockBuilder}, converting it to the
     * target column type. Partition key values are always stored as strings in {@link HivePartitionKey}.
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
            BigDecimal bigDecimal = SqlDecimal.decimal(value, decimalType).toBigDecimal();
            if (decimalType.isShort()) {
                writeShortDecimal(blockBuilder, bigDecimal.unscaledValue().longValue());
            }
            else {
                writeBigDecimal(decimalType, blockBuilder, bigDecimal);
            }
        }
        else if (targetType instanceof DateType dateType) {
            try {
                dateType.writeInt(blockBuilder, toIntExact(LocalDate.parse(value).toEpochDay()));
            }
            catch (DateTimeParseException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Invalid date string format for DATE type: '%s'. Expected format like YYYY-MM-DD. Details: %s",
                                value, e.getMessage()), e);
            }
            catch (ArithmeticException e) {
                throw new TrinoException(GENERIC_INTERNAL_ERROR,
                        format("Date string '%s' results in a day count out of integer range for DATE type. Details: %s",
                                value, e.getMessage()), e);
            }
        }
        else {
            throw new TrinoException(GENERIC_INTERNAL_ERROR,
                    format("Unknown target type '%s' for partition key value '%s'", targetType, value));
        }
    }
}
