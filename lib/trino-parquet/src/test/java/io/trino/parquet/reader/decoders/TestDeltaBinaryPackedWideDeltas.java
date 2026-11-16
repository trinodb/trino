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
package io.trino.parquet.reader.decoders;

import io.trino.parquet.PrimitiveField;
import io.trino.parquet.reader.SimpleSliceInputStream;
import org.apache.parquet.column.values.ValuesWriter;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.OptionalInt;

import static io.trino.parquet.ParquetEncoding.DELTA_BINARY_PACKED;
import static io.trino.parquet.reader.TestingValuesWriters.getValuesWriter;
import static io.trino.parquet.reader.decoders.AbstractValueDecodersTest.createField;
import static io.trino.parquet.reader.decoders.AbstractValueDecodersTest.getWrittenBuffer;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * The deltas of an INT32 column are int32, so for TINYINT and SMALLINT a delta can be wider than the
 * column type while the running sum of the values still stays in range. A miniblock whose deltas are
 * all equal has a bit width of 0, and its decoding must not require the delta to fit the column type.
 */
public class TestDeltaBinaryPackedWideDeltas
{
    @Test
    public void testByteDeltaWiderThanByte()
    {
        assertByteValues(Byte.MIN_VALUE, Byte.MAX_VALUE);
        assertByteValues(Byte.MAX_VALUE, Byte.MIN_VALUE);
        // a full miniblock of zero deltas followed by a miniblock with a single wide delta
        byte[] values = new byte[33];
        Arrays.fill(values, Byte.MIN_VALUE);
        values[32] = Byte.MAX_VALUE;
        assertByteValues(values);
    }

    @Test
    public void testShortDeltaWiderThanShort()
    {
        assertShortValues(Short.MIN_VALUE, Short.MAX_VALUE);
        assertShortValues(Short.MAX_VALUE, Short.MIN_VALUE);
        short[] values = new short[33];
        Arrays.fill(values, Short.MIN_VALUE);
        values[32] = Short.MAX_VALUE;
        assertShortValues(values);
    }

    private static void assertByteValues(byte... expected)
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), TINYINT);
        ValuesWriter writer = getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty());
        for (byte value : expected) {
            writer.writeInteger(value);
        }
        ValueDecoder<byte[]> decoder = new ValueDecoders(field).getByteDecoder(DELTA_BINARY_PACKED);
        decoder.init(new SimpleSliceInputStream(getWrittenBuffer(writer).dataPage()));

        byte[] actual = new byte[expected.length];
        decoder.read(actual, 0, expected.length);
        assertThat(actual).containsExactly(expected);
    }

    private static void assertShortValues(short... expected)
    {
        PrimitiveField field = createField(INT32, OptionalInt.empty(), SMALLINT);
        ValuesWriter writer = getValuesWriter(DELTA_BINARY_PACKED, INT32, OptionalInt.empty());
        for (short value : expected) {
            writer.writeInteger(value);
        }
        ValueDecoder<short[]> decoder = new ValueDecoders(field).getShortDecoder(DELTA_BINARY_PACKED);
        decoder.init(new SimpleSliceInputStream(getWrittenBuffer(writer).dataPage()));

        short[] actual = new short[expected.length];
        decoder.read(actual, 0, expected.length);
        assertThat(actual).containsExactly(expected);
    }
}
