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
package io.trino.spi.predicate;

import io.trino.spi.type.Int128;
import io.trino.spi.type.TrinoNumber;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToIntBits;
import static java.lang.Float.floatToRawIntBits;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.TEN;
import static java.math.BigDecimal.ZERO;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValueSet
{
    @Test
    public void testRejectNullOnCreate()
    {
        testRejectNullOnCreate(BOOLEAN, true, false, true);
        testRejectNullOnCreate(TINYINT, 1L, 2L, 3L);
        testRejectNullOnCreate(SMALLINT, 1L, 2L, 3L);
        testRejectNullOnCreate(INTEGER, 1L, 2L, 3L);
        testRejectNullOnCreate(BIGINT, 1L, 2L, 3L);
        testRejectNullOnCreate(REAL, (long) floatToIntBits(1.0f), (long) floatToIntBits(2.0f), (long) floatToIntBits(3.0f));
        testRejectNullOnCreate(DOUBLE, 1.0, 2.0, 3.0);
        testRejectNullOnCreate(createDecimalType(10, 2), 1L, 2L, 3L);
        testRejectNullOnCreate(createDecimalType(20, 2), Int128.valueOf(1), Int128.valueOf(2), Int128.valueOf(3));
        testRejectNullOnCreate(NUMBER, TrinoNumber.from(ZERO), TrinoNumber.from(ONE), TrinoNumber.from(TEN));
        testRejectNullOnCreate(VARCHAR, utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(createVarcharType(25), utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
        testRejectNullOnCreate(VARBINARY, utf8Slice("abc"), utf8Slice("def"), utf8Slice("ghi"));
    }

    private static void testRejectNullOnCreate(Type type, Object exampleValueA, Object exampleValueB, Object exampleValueC)
    {
        assertThatThrownBy(() -> ValueSet.of(type, null))
                .as("ValueSet.of %s with null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.of(type, null, exampleValueA, exampleValueB, exampleValueC))
                .as("ValueSet.of %s with null value and non-null values", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.of(type, exampleValueA, exampleValueB, null, exampleValueC))
                .as("ValueSet.of %s with non-null values and a null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.copyOf(type, asList(exampleValueA, exampleValueB, null, exampleValueC)))
                .as("ValueSet.copyOf %s with non-null values and a null value", type)
                .isInstanceOf(NullPointerException.class)
                .hasMessage("value is null");
    }

    @Test
    public void testDoubleNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThat(ValueSet.of(DOUBLE, Double.NaN).containsValue(Double.NaN))
                .isTrue();

        // a different NaN
        assertThat(ValueSet.of(DOUBLE, longBitsToDouble(0x7FF8123412341234L)).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.of with others and NaN
        assertThat(ValueSet.of(DOUBLE, 42., 123., Double.NaN, 127.).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.of with others and NaN first
        assertThat(ValueSet.of(DOUBLE, Double.NaN, 42., 123., 127.).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.copyOf with NaN
        assertThat(ValueSet.copyOf(DOUBLE, List.of(42., 123., Double.NaN, 127.)).containsValue(Double.NaN))
                .isTrue();

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThat(ValueSet.copyOf(DOUBLE, List.of(Double.NaN, 42., 123., 127.)).containsValue(Double.NaN))
                .isTrue();
    }

    @Test
    public void testRealNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThat(ValueSet.of(REAL, real(Float.NaN)).containsValue(real(Float.NaN)))
                .isTrue();

        // a different NaN
        assertThat(ValueSet.of(REAL, (long) 0x7FC01234).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.of with others and NaN
        assertThat(ValueSet.of(REAL, real(42), real(123), real(Float.NaN), real(127)).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.of with others and NaN first
        assertThat(ValueSet.of(REAL, real(Float.NaN), real(42), real(123), real(127)).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.copyOf with NaN
        assertThat(ValueSet.copyOf(REAL, List.of(real(42), real(123), real(Float.NaN), real(127))).containsValue(real(Float.NaN)))
                .isTrue();

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThat(ValueSet.copyOf(REAL, List.of(real(Float.NaN), real(42), real(123), real(127))).containsValue(real(Float.NaN)))
                .isTrue();
    }

    private static long real(float value)
    {
        return floatToRawIntBits(value);
    }
}
