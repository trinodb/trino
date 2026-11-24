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
package io.trino.spi.block;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TypeOperators;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static org.assertj.core.api.Assertions.assertThat;

final class TestBufferedValueBuilders
{
    private static final int ITERATIONS = 100_000;

    @Test
    void testArrayBuilderSizing()
    {
        BufferedArrayValueBuilder builder = BufferedArrayValueBuilder.createBuffered(new ArrayType(BIGINT));
        Block first = buildArray(builder, -1);
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(1024);

        for (int i = 0; i < ITERATIONS; i++) {
            buildArray(builder, i);
        }
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(16 * 1024);
        assertThat(BIGINT.getLong(first, 0)).isEqualTo(-1);
        assertThat(BIGINT.getLong(first, 1)).isEqualTo(0);
    }

    @Test
    void testMapBuilderSizing()
    {
        BufferedMapValueBuilder builder = BufferedMapValueBuilder.createBuffered(new MapType(BIGINT, BIGINT, new TypeOperators()));
        SqlMap first = buildMap(builder, -1);
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(1024);

        for (int i = 0; i < ITERATIONS; i++) {
            buildMap(builder, i);
        }
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(32 * 1024);
        assertThat(BIGINT.getLong(first.getRawKeyBlock(), first.getRawOffset())).isEqualTo(-1);
        assertThat(BIGINT.getLong(first.getRawValueBlock(), first.getRawOffset() + 1)).isEqualTo(2);
    }

    @Test
    void testRowBuilderSizing()
    {
        BufferedRowValueBuilder builder = BufferedRowValueBuilder.createBuffered(RowType.anonymous(List.of(BIGINT, BIGINT)));
        SqlRow first = buildRow(builder, -1);
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(1024);

        for (int i = 0; i < ITERATIONS; i++) {
            buildRow(builder, i);
        }
        assertThat(builder.getRetainedSizeInBytes()).isLessThan(32 * 1024);
        assertThat(BIGINT.getLong(first.getRawFieldBlock(0), first.getRawIndex())).isEqualTo(-1);
        assertThat(BIGINT.getLong(first.getRawFieldBlock(1), first.getRawIndex())).isEqualTo(0);
    }

    private static Block buildArray(BufferedArrayValueBuilder builder, long value)
    {
        return builder.build(2, elements -> {
            BIGINT.writeLong(elements, value);
            BIGINT.writeLong(elements, value + 1);
        });
    }

    private static SqlMap buildMap(BufferedMapValueBuilder builder, long value)
    {
        return builder.build(2, (keys, values) -> {
            BIGINT.writeLong(keys, value);
            BIGINT.writeLong(values, value + 2);
            BIGINT.writeLong(keys, value + 1);
            BIGINT.writeLong(values, value + 3);
        });
    }

    private static SqlRow buildRow(BufferedRowValueBuilder builder, long value)
    {
        return builder.build(fields -> {
            BIGINT.writeLong(fields.get(0), value);
            BIGINT.writeLong(fields.get(1), value + 1);
        });
    }
}
