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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.trino.parquet.metadata.ColumnChunkMetadata;
import io.trino.plugin.deltalake.transactionlog.statistics.DeltaLakeFileStatistics;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.EncodingStats;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static io.trino.plugin.deltalake.DeltaLakeWriter.mergeStats;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.lang.Float.floatToRawIntBits;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDeltaLakeWriter
{
    @Test
    public void testMergeIntStatistics()
    {
        String columnName = "t_int";
        PrimitiveType intType = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.INT32, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, intType, 10,
                        Statistics.getBuilderForReading(intType).withMin(getIntByteArray(-100)).withMax(getIntByteArray(250)).withNumNulls(6).build()),
                createMetaData(columnName, intType, 10,
                        Statistics.getBuilderForReading(intType).withMin(getIntByteArray(-200)).withMax(getIntByteArray(150)).withNumNulls(7).build()));
        DeltaLakeColumnHandle intColumn = new DeltaLakeColumnHandle(columnName, INTEGER, OptionalInt.empty(), columnName, INTEGER, REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, INTEGER), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(intColumn)).isEqualTo(Optional.of(-200L));
        assertThat(fileStats.getMaxColumnValue(intColumn)).isEqualTo(Optional.of(250L));
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.of(13L));
    }

    @Test
    public void testMergeFloatStatistics()
    {
        String columnName = "t_float";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getFloatByteArray(0.01f)).withMax(getFloatByteArray(1.0f)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getFloatByteArray(-2.001f)).withMax(getFloatByteArray(0.0f)).withNumNulls(7).build()));
        DeltaLakeColumnHandle floatColumn = new DeltaLakeColumnHandle(columnName, REAL, OptionalInt.empty(), columnName, REAL, REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, REAL), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(floatColumn)).isEqualTo(Optional.of((long) floatToRawIntBits(-2.001f)));
        assertThat(fileStats.getMaxColumnValue(floatColumn)).isEqualTo(Optional.of((long) floatToRawIntBits(1.0f)));
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.of(13L));
    }

    @Test
    public void testMergeFloatNaNStatistics()
    {
        String columnName = "t_float";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.FLOAT, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getFloatByteArray(0.01f)).withMax(getFloatByteArray(1.0f)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getFloatByteArray(Float.NaN)).withMax(getFloatByteArray(1.0f)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getFloatByteArray(-2.001f)).withMax(getFloatByteArray(0.0f)).withNumNulls(7).build()));
        DeltaLakeColumnHandle floatColumn = new DeltaLakeColumnHandle(columnName, REAL, OptionalInt.empty(), columnName, REAL, REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, REAL), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(floatColumn)).isEqualTo(Optional.empty());
        assertThat(fileStats.getMaxColumnValue(floatColumn)).isEqualTo(Optional.empty());
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.empty());
    }

    @Test
    public void testMergeDoubleNaNStatistics()
    {
        String columnName = "t_double";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.DOUBLE, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getDoubleByteArray(0.01f)).withMax(getDoubleByteArray(1.0f)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getDoubleByteArray(Double.NaN)).withMax(getDoubleByteArray(1.0f)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin(getDoubleByteArray(-2.001f)).withMax(getDoubleByteArray(0.0f)).withNumNulls(7).build()));
        DeltaLakeColumnHandle doubleColumn = new DeltaLakeColumnHandle(columnName, DOUBLE, OptionalInt.empty(), columnName, DOUBLE, REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, DOUBLE), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(doubleColumn)).isEqualTo(Optional.empty());
        assertThat(fileStats.getMaxColumnValue(doubleColumn)).isEqualTo(Optional.empty());
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.empty());
    }

    @Test
    public void testMergeStringStatistics()
    {
        String columnName = "t_string";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin("aba".getBytes(UTF_8)).withMax("ab⌘".getBytes(UTF_8)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin("aba".getBytes(UTF_8)).withMax("abc".getBytes(UTF_8)).withNumNulls(6).build()));
        DeltaLakeColumnHandle varcharColumn = new DeltaLakeColumnHandle(columnName, VarcharType.createUnboundedVarcharType(), OptionalInt.empty(), columnName, VarcharType.createUnboundedVarcharType(), REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, createUnboundedVarcharType()), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(varcharColumn)).isEqualTo(Optional.of(utf8Slice("aba")));
        assertThat(fileStats.getMaxColumnValue(varcharColumn)).isEqualTo(Optional.of(utf8Slice("ab⌘")));
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.of(12L));
    }

    @Test
    public void testMergeStringUnicodeStatistics()
    {
        String columnName = "t_string";
        PrimitiveType type = new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, columnName);
        List<ColumnChunkMetadata> metadata = ImmutableList.of(
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin("aba".getBytes(UTF_8)).withMax("ab\uFAD8".getBytes(UTF_8)).withNumNulls(6).build()),
                createMetaData(columnName, type, 10,
                        Statistics.getBuilderForReading(type).withMin("aba".getBytes(UTF_8)).withMax("ab\uD83D\uDD74".getBytes(UTF_8)).withNumNulls(6).build()));
        DeltaLakeColumnHandle varcharColumn = new DeltaLakeColumnHandle(columnName, VarcharType.createUnboundedVarcharType(), OptionalInt.empty(), columnName, VarcharType.createUnboundedVarcharType(), REGULAR, Optional.empty());

        DeltaLakeFileStatistics fileStats = mergeStats(buildMultimap(columnName, metadata), ImmutableMap.of(columnName, createUnboundedVarcharType()), 20);
        assertThat(fileStats.getNumRecords()).isEqualTo(Optional.of(20L));
        assertThat(fileStats.getMinColumnValue(varcharColumn)).isEqualTo(Optional.of(utf8Slice("aba")));
        assertThat(fileStats.getMaxColumnValue(varcharColumn)).isEqualTo(Optional.of(utf8Slice("ab\uD83D\uDD74")));
        assertThat(fileStats.getNullCount(columnName)).isEqualTo(Optional.of(12L));
    }

    private ColumnChunkMetadata createMetaData(String columnName, PrimitiveType columnType, long valueCount, Statistics<?> statistics)
    {
        return ColumnChunkMetadata.get(
                ColumnPath.fromDotString(columnName),
                columnType,
                CompressionCodecName.SNAPPY,
                new EncodingStats.Builder().build(),
                ImmutableSet.of(),
                statistics,
                0,
                0,
                valueCount,
                0,
                0);
    }

    private Multimap<String, ColumnChunkMetadata> buildMultimap(String columnName, List<ColumnChunkMetadata> metadata)
    {
        return ImmutableMultimap.<String, ColumnChunkMetadata>builder()
                .putAll(columnName, metadata)
                .build();
    }

    static byte[] getIntByteArray(int i)
    {
        return ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(i).array();
    }

    static byte[] getFloatByteArray(float f)
    {
        return ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putFloat(f).array();
    }

    static byte[] getDoubleByteArray(double d)
    {
        return ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putDouble(d).array();
    }
}
