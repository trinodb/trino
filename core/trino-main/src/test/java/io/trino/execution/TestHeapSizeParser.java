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
package io.trino.execution;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.function.Supplier;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHeapSizeParser
{
    private static final Supplier<Long> MAX_HEAP_MEMORY = () -> DataSize.of(8, GIGABYTE).toBytes();
    private static final HeapSizeParser HEAP_SIZE_PARSER = new HeapSizeParser(MAX_HEAP_MEMORY);

    @Test
    void testAbsoluteMemory()
    {
        assertThat(HEAP_SIZE_PARSER.parse("1GB")).isEqualTo(DataSize.of(1, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("8GB")).isEqualTo(DataSize.of(8, GIGABYTE));

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("9GB"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be greater than maximum heap size");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("0B"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");
    }

    @Test
    void testRelativeMemory()
    {
        assertThat(HEAP_SIZE_PARSER.parse("12.5%")).isEqualTo(DataSize.of(1, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("100%")).isEqualTo(DataSize.of(8, GIGABYTE));
        assertThat(HEAP_SIZE_PARSER.parse("50%")).isEqualTo(DataSize.of(4, GIGABYTE));

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("125%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be greater than maximum heap size");

        assertThatThrownBy(() -> HEAP_SIZE_PARSER.parse("0%"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Heap size cannot be less than or equal to 0");
    }
}
