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

import io.airlift.slice.Slice;
import io.trino.spi.type.VarcharType;
import org.testng.annotations.Test;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static java.lang.Double.longBitsToDouble;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestValueSet
{
    @Test
    public void testRejectNullOnCreate()
    {
        // Using varchar for the test since its stack representation is not primitive, so a null is more likely to go unnoticed.
        VarcharType varcharType = createVarcharType(25);
        Slice abc = utf8Slice("abc");
        Slice def = utf8Slice("def");
        Slice ghi = utf8Slice("ghi");

        assertThatThrownBy(() -> ValueSet.of(varcharType, null))
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.of(varcharType, abc, def, null, ghi))
                .hasMessage("value is null");

        assertThatThrownBy(() -> ValueSet.copyOf(varcharType, asList(abc, def, null, ghi)))
                .hasMessage("value is null");
    }

    @Test
    public void testRejectDoubleNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThatThrownBy(() -> ValueSet.of(DOUBLE, Double.NaN))
                .hasMessage("cannot use NaN as range bound");

        // a different NaN
        assertThatThrownBy(() -> ValueSet.of(DOUBLE, longBitsToDouble(0x7ff8123412341234L)))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.of with others and NaN
        assertThatThrownBy(() -> ValueSet.of(DOUBLE, 42., 123., Double.NaN, 127.))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.of with others and NaN first
        assertThatThrownBy(() -> ValueSet.of(DOUBLE, Double.NaN, 42., 123., 127.))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.copyOf with NaN
        assertThatThrownBy(() -> ValueSet.copyOf(DOUBLE, List.of(42., 123., Double.NaN, 127.)))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThatThrownBy(() -> ValueSet.copyOf(DOUBLE, List.of(Double.NaN, 42., 123., 127.)))
                .hasMessage("cannot use NaN as range bound");
    }

    @Test
    public void testRejectRealNaNOnCreate()
    {
        // ValueSet.of with NaN
        assertThatThrownBy(() -> ValueSet.of(REAL, real(Float.NaN)))
                .hasMessage("cannot use NaN as range bound");

        // a different NaN
        assertThatThrownBy(() -> ValueSet.of(REAL, (long) 0x7fc01234))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.of with others and NaN
        assertThatThrownBy(() -> ValueSet.of(REAL, real(42), real(123), real(Float.NaN), real(127)))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.of with others and NaN first
        assertThatThrownBy(() -> ValueSet.of(REAL, real(Float.NaN), real(42), real(123), real(127)))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.copyOf with NaN
        assertThatThrownBy(() -> ValueSet.copyOf(REAL, List.of(real(42), real(123), real(Float.NaN), real(127))))
                .hasMessage("cannot use NaN as range bound");

        // ValueSet.copyOf with NaN first, in case the method has a special treatment for first value
        assertThatThrownBy(() -> ValueSet.copyOf(REAL, List.of(real(Float.NaN), real(42), real(123), real(127))))
                .hasMessage("cannot use NaN as range bound");
    }

    private static long real(float value)
    {
        return floatToRawIntBits(value);
    }
}
