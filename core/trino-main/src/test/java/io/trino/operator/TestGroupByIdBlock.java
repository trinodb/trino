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
package io.trino.operator;

import org.testng.annotations.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestGroupByIdBlock
{
    private static final long[] BUFFER = IntStream.range(0, 10)
            .mapToLong(i -> i)
            .toArray();

    @Test
    public void testGetLong()
    {
        GroupByIdBlock block = GroupByIdBlock.ofArray(0, BUFFER);
        for (int i = 0; i < 10; i++) {
            assertThat(block.getLong(i, 0)).isEqualTo(BUFFER[i]);
        }

        assertThatThrownBy(() -> block.getLong(0, 1)).hasMessageContaining("offset must be zero");
        assertThatThrownBy(() -> block.getLong(-1, 0)).hasMessageContaining("Invalid position");
        assertThatThrownBy(() -> block.getLong(10, 0)).hasMessageContaining("Invalid position");
    }

    @Test
    public void testGetLongRle()
    {
        GroupByIdBlock block = GroupByIdBlock.rle(0, 42, 10);
        for (int i = 0; i < 10; i++) {
            assertThat(block.getLong(i, 0)).isEqualTo(42);
        }

        assertThatThrownBy(() -> block.getLong(0, 1)).hasMessageContaining("offset must be zero");
        assertThatThrownBy(() -> block.getLong(-1, 0)).hasMessageContaining("Invalid position");
        assertThatThrownBy(() -> block.getLong(10, 0)).hasMessageContaining("Invalid position");
    }

    @Test
    public void testCopyPositions()
    {
        GroupByIdBlock block = GroupByIdBlock.ofArray(0, BUFFER);
        GroupByIdBlock rleBlock = GroupByIdBlock.rle(0, 42, 10);
        int[] positions = new int[] {5, 4, 3, 2, 1};

        GroupByIdBlock copied = block.copyPositions(positions, 1, 3);
        GroupByIdBlock rleCopied = rleBlock.copyPositions(positions, 1, 3);
        assertThat(copied.getPositionCount()).isEqualTo(3);
        assertThat(rleCopied.getPositionCount()).isEqualTo(3);

        assertThat(copied.getGroupId(0)).isEqualTo(4);
        assertThat(copied.getGroupId(1)).isEqualTo(3);
        assertThat(copied.getGroupId(2)).isEqualTo(2);
        assertThat(rleCopied.isRle()).isTrue();
        assertThat(rleCopied.getGroupId(0)).isEqualTo(42);
    }
}
