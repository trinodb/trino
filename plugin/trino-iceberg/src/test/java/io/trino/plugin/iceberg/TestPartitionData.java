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
package io.trino.plugin.iceberg;

import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestPartitionData
{
    @Test
    public void testJsonRoundTrip()
    {
        Type[] types = {
                Types.BooleanType.get(),
                Types.IntegerType.get(),
                Types.LongType.get(),
                Types.FloatType.get(),
                Types.DoubleType.get(),
                Types.StringType.get(),
                Types.UUIDType.get(),
                Types.BinaryType.get(),
                Types.DecimalType.of(10, 2),
                Types.LongType.get(),
        };
        UUID uuid = UUID.randomUUID();
        PartitionData expected = new PartitionData(new Object[] {
                true,
                7,
                8_000_000_000L,
                1.5f,
                2.25,
                "abc",
                uuid,
                new byte[] {1, 2, 3},
                new BigDecimal("12.34"),
                null,
        });

        PartitionData actual = PartitionData.fromJson(PartitionData.toJson(expected), types);

        assertThat(actual.get(0, Boolean.class)).isTrue();
        assertThat(actual.get(1, Integer.class)).isEqualTo(7);
        assertThat(actual.get(2, Long.class)).isEqualTo(8_000_000_000L);
        assertThat(actual.get(3, Float.class)).isEqualTo(1.5f);
        assertThat(actual.get(4, Double.class)).isEqualTo(2.25);
        assertThat(actual.get(5, String.class)).isEqualTo("abc");
        assertThat(actual.get(6, UUID.class)).isEqualTo(uuid);
        assertThat(actual.get(7, byte[].class)).containsExactly(1, 2, 3);
        assertThat(actual.get(8, BigDecimal.class)).isEqualByComparingTo("12.34");
        assertThat(actual.get(9, Long.class)).isNull();
    }

    @Test
    public void testNaN()
    {
        PartitionData actual = PartitionData.fromJson(
                PartitionData.toJson(new PartitionData(new Object[] {Float.NaN, Double.NaN})),
                new Type[] {Types.FloatType.get(), Types.DoubleType.get()});

        assertThat(actual.get(0, Float.class)).isNaN();
        assertThat(actual.get(1, Double.class)).isNaN();
    }

    @Test
    public void testInfinity()
    {
        PartitionData actual = PartitionData.fromJson(
                PartitionData.toJson(new PartitionData(new Object[] {Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY})),
                new Type[] {Types.FloatType.get(), Types.DoubleType.get()});

        assertThat(actual.get(0, Float.class)).isEqualTo(Float.POSITIVE_INFINITY);
        assertThat(actual.get(1, Double.class)).isEqualTo(Double.NEGATIVE_INFINITY);
    }

    @Test
    public void testMalformedJson()
    {
        assertThatThrownBy(() -> PartitionData.fromJson("{\"partitionValues\":[", new Type[] {Types.LongType.get()}))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Conversion from JSON failed for PartitionData");
    }
}
