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
package io.trino.spi.function;

import io.trino.spi.block.Block;
import io.trino.spi.block.IntArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Optional;

import static io.trino.spi.function.SelectedPositions.positionsList;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestSelectedPositions
{
    @Test
    void testSelectDoesNotRetainPositions()
    {
        int[] positions = {0, 3, 1};
        Block selected = positionsList(positions, 1, 2).select(new IntArrayBlock(4, Optional.empty(), new int[] {10, 11, 12, 13}));
        // The caller may reuse the positions array for the next page
        Arrays.fill(positions, 2);
        assertThat(INTEGER.getInt(selected, 0)).isEqualTo(13);
        assertThat(INTEGER.getInt(selected, 1)).isEqualTo(11);
    }
}
