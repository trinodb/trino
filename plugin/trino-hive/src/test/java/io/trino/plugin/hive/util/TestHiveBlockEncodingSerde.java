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
package io.trino.plugin.hive.util;

import io.airlift.slice.BasicSliceInput;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.UncheckedIOException;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveBlockEncodingSerde
{
    @Test
    public void testSerialization()
    {
        testSerialization(BOOLEAN);
        testSerialization(TINYINT);
        testSerialization(SMALLINT);
        testSerialization(INTEGER);
        testSerialization(BIGINT);
        testSerialization(REAL);
        testSerialization(DOUBLE);
        testSerialization(createDecimalType(7, 2));
        testSerialization(createDecimalType(37, 2));
        testSerialization(createCharType(10));
        testSerialization(createVarcharType(10));
        testSerialization(createUnboundedVarcharType());
        testSerialization(DATE);
        testSerialization(createTimeType(0));
        testSerialization(createTimeType(3));
        testSerialization(createTimeType(12));
        testSerialization(createTimeWithTimeZoneType(0));
        testSerialization(createTimeWithTimeZoneType(3));
        testSerialization(createTimeWithTimeZoneType(12));
        testSerialization(createTimestampType(0));
        testSerialization(createTimestampType(3));
        testSerialization(createTimestampType(12));
        testSerialization(createTimestampWithTimeZoneType(0));
        testSerialization(createTimestampWithTimeZoneType(3));
        testSerialization(createTimestampWithTimeZoneType(12));
    }

    private void testSerialization(Type type)
    {
        Slice serialized;
        try (SliceOutput sliceOutput = new DynamicSliceOutput(0)) {
            Block block = type.createBlockBuilder(null, 0).build();
            new HiveBlockEncodingSerde().writeBlock(sliceOutput, block);
            serialized = sliceOutput.slice();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (BasicSliceInput input = serialized.getInput()) {
            Block read = new HiveBlockEncodingSerde().readBlock(input);
            assertThat(read.getPositionCount()).isEqualTo(0);
        }
    }
}
