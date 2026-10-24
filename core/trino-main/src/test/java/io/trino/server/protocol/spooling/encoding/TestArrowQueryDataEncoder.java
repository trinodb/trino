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
package io.trino.server.protocol.spooling.encoding;

import com.google.common.collect.ImmutableMap;
import io.trino.server.protocol.OutputColumn;
import io.trino.server.protocol.spooling.QueryDataEncoder;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.LongTimestamp;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.TimeStampVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.util.Text;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.client.spooling.DataAttribute.SEGMENT_SIZE;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_PICOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.UuidType.javaUuidToTrinoUuid;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static io.trino.util.StructuralTestUtil.arrayBlockOf;
import static io.trino.util.StructuralTestUtil.mapType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestArrowQueryDataEncoder
{
    @Test
    void testPrimitiveTypes()
    {
        assertRoundTrip(BOOLEAN, values(true, null, false), values(true, null, false));
        assertRoundTrip(INTEGER, values(1L, null, -7L), values(1, null, -7));
        assertRoundTrip(BIGINT, values(Long.MAX_VALUE, null), values(Long.MAX_VALUE, null));
        assertRoundTrip(REAL, values(1.5f, null), values(1.5f, null));
        assertRoundTrip(DOUBLE, values(2.25, null), values(2.25, null));
        assertRoundTrip(VARCHAR, values("abc", null, ""), values("abc", null, ""));
        assertRoundTrip(createCharType(3), values("a"), values("a  "));
        assertRoundTrip(DATE, values(19000L, null), values(19000, null));
        assertRoundTrip(createDecimalType(10, 2), values(12345L, null), values(new BigDecimal("123.45"), null));
        assertRoundTrip(createDecimalType(30, 2), values(Int128.valueOf(-12345)), values(new BigDecimal("-123.45")));

        byte[] binary = (byte[]) encodeAndRead(VARBINARY, values((Object) new byte[] {1, 2})).getFirst();
        assertThat(binary).containsExactly(1, 2);

        UUID uuid = UUID.fromString("12151fd2-7586-11e9-8f9e-2a86e4085a59");
        byte[] actual = (byte[]) encodeAndRead(UuidType.UUID, values(javaUuidToTrinoUuid(uuid))).getFirst();
        assertThat(actual).isEqualTo(ByteBuffer.allocate(16).putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits()).array());
        assertThat(ArrowWriters.toField("c", UuidType.UUID).getType()).isEqualTo(new ArrowType.FixedSizeBinary(16));
        assertThat(ArrowWriters.toField("c", UuidType.UUID).getMetadata()).containsEntry("ARROW:extension:name", "arrow.uuid");
    }

    @Test
    void testTemporalTypes()
    {
        // floor division keeps pre-epoch values on the right side
        assertRawLongs(TIMESTAMP_MILLIS, values(-1_500L, 2_000L), -2, 2);
        assertRawLongs(TIMESTAMP_NANOS, values(new LongTimestamp(1, 500_000)), 1_500);
        assertRawLongs(TIMESTAMP_TZ_MILLIS, values(packDateTimeWithZone(1_234, UTC_KEY)), 1_234);
        assertRoundTrip(TIME_MICROS, values(3_000_000L), values(3L)); // picos -> micros
        assertThat(encodeAndRead(INTERVAL_YEAR_MONTH, values(14L)).getFirst().toString()).isEqualTo("P14M PT0S");
    }

    @Test
    void testStructuralTypes()
    {
        assertRoundTrip(new ArrayType(BIGINT), values(List.of(1L, 2L), null, List.of()), values(List.of(1L, 2L), null, List.of()));

        RowType rowType = RowType.from(List.of(RowType.field("a", BIGINT), RowType.field("b", VARCHAR)));
        List<Object> rows = encodeAndRead(rowType, values(Arrays.asList(1L, "x"), null));
        assertThat(rows.getFirst().toString()).isEqualTo("{\"a\":1,\"b\":\"x\"}");
        assertThat(rows.get(1)).isNull();

        List<Object> maps = encodeAndRead(mapType(VARCHAR, BIGINT), values(ImmutableMap.of("k", 1L), null));
        assertThat(maps.getFirst().toString()).isEqualTo("[{\"key\":\"k\",\"value\":1}]");
        assertThat(maps.get(1)).isNull();
    }

    @Test
    void testMultiplePages()
            throws IOException
    {
        Page page = new Page(arrayBlockOf(BIGINT, 1L, 2L));
        byte[] bytes = encode(List.of(new OutputColumn(0, "c", BIGINT)), List.of(page, page, page));
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator)) {
            int batches = 0;
            while (reader.loadNextBatch()) {
                assertThat(reader.getVectorSchemaRoot().getRowCount()).isEqualTo(2);
                batches++;
            }
            assertThat(batches).isEqualTo(3);
        }
    }

    @Test
    void testUnsupportedType()
    {
        assertThatThrownBy(() -> new ArrowQueryDataEncoder.Factory().create(TEST_SESSION, List.of(new OutputColumn(0, "c", TIMESTAMP_PICOS))))
                .isInstanceOfSatisfying(TrinoException.class, e -> assertThat(e.getErrorCode()).isEqualTo(NOT_SUPPORTED.toErrorCode()))
                .hasMessageContaining("timestamp(12)");
    }

    private static void assertRoundTrip(Type type, Object[] input, Object[] expected)
    {
        List<Object> actual = encodeAndRead(type, input).stream()
                .map(value -> value instanceof Text ? value.toString() : value)
                .toList();
        assertThat(actual).containsExactly(expected);
    }

    private static void assertRawLongs(Type type, Object[] input, long... expected)
    {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = read(type, input, allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            TimeStampVector vector = (TimeStampVector) reader.getVectorSchemaRoot().getVector(0);
            for (int i = 0; i < expected.length; i++) {
                assertThat(vector.get(i)).isEqualTo(expected[i]);
            }
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<Object> encodeAndRead(Type type, Object[] input)
    {
        try (BufferAllocator allocator = new RootAllocator();
                ArrowStreamReader reader = read(type, input, allocator)) {
            assertThat(reader.loadNextBatch()).isTrue();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            FieldVector vector = root.getVector(0);
            List<Object> values = new ArrayList<>();
            for (int i = 0; i < root.getRowCount(); i++) {
                values.add(vector.getObject(i));
            }
            assertThat(reader.loadNextBatch()).isFalse();
            return values;
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static ArrowStreamReader read(Type type, Object[] input, BufferAllocator allocator)
            throws IOException
    {
        Block block = arrayBlockOf(type, input);
        byte[] bytes = encode(List.of(new OutputColumn(0, "c", type)), List.of(new Page(block)));
        return new ArrowStreamReader(new ByteArrayInputStream(bytes), allocator);
    }

    private static byte[] encode(List<OutputColumn> columns, List<Page> pages)
            throws IOException
    {
        QueryDataEncoder encoder = new ArrowQueryDataEncoder.Factory().create(TEST_SESSION, columns);
        try {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            int size = encoder.encodeTo(output, pages).get(SEGMENT_SIZE, Integer.class);
            assertThat(output.size()).isEqualTo(size);
            return output.toByteArray();
        }
        finally {
            encoder.close();
        }
    }

    private static Object[] values(Object... values)
    {
        return values;
    }
}
