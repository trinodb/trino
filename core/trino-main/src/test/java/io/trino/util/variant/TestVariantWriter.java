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
package io.trino.util.variant;

import io.airlift.slice.Slice;
import io.trino.spi.type.Type;
import io.trino.spi.variant.Variant;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.Float.floatToRawIntBits;
import static org.assertj.core.api.Assertions.assertThat;

class TestVariantWriter
{
    @Test
    void testPrimitiveTypes()
    {
        assertPrimitiveWrite(BOOLEAN, true, Variant.ofBoolean(true));
        assertPrimitiveWrite(TINYINT, 12L, Variant.ofByte((byte) 12));
        assertPrimitiveWrite(SMALLINT, 1234L, Variant.ofShort((short) 1234));
        assertPrimitiveWrite(INTEGER, 123_456L, Variant.ofInt(123_456));
        assertPrimitiveWrite(BIGINT, 123_456_789L, Variant.ofLong(123_456_789L));
        assertPrimitiveWrite(REAL, (long) floatToRawIntBits(12.5f), Variant.ofFloat(12.5f));
        assertPrimitiveWrite(DOUBLE, 12.5, Variant.ofDouble(12.5));
        assertPrimitiveWrite(DATE, 20_000L, Variant.ofDate(20_000));
        assertPrimitiveWrite(VARCHAR, "hello", Variant.ofString("hello"));

        Slice binary = utf8Slice("hello");
        assertPrimitiveWrite(VARBINARY, binary, Variant.ofBinary(binary));
    }

    @Test
    void testPrimitiveNull()
    {
        assertPrimitiveWrite(BOOLEAN, null, Variant.NULL_VALUE);
        assertPrimitiveWrite(VARCHAR, null, Variant.NULL_VALUE);
    }

    private static void assertPrimitiveWrite(Type type, Object value, Variant expected)
    {
        assertThat(VariantWriter.create(type).write(value))
                .isEqualTo(expected);
    }
}
