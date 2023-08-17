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
package io.trino.plugin.memory;

import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import io.trino.spi.block.LongArrayBlock;

import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public final class TestUtils
{
    private TestUtils() {}

    public static void assertBlockEquals(Block actual, Block expected)
    {
        assertThat(actual.getPositionCount()).isEqualTo(expected.getPositionCount());
        if (expected instanceof IntArrayBlock) {
            assertThat(actual).isInstanceOf(IntArrayBlock.class);
            for (int position = 0; position < actual.getPositionCount(); position++) {
                assertThat(INTEGER.getInt(actual, position)).isEqualTo(INTEGER.getInt(expected, position));
            }
        }
        else {
            assertThat(actual).isInstanceOf(LongArrayBlock.class);
            for (int position = 0; position < actual.getPositionCount(); position++) {
                assertThat(actual.getLong(position, 0)).isEqualTo(expected.getLong(position, 0));
            }
        }
    }
}
