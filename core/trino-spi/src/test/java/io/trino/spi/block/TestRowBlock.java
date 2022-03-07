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

import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestRowBlock
{
    @Test
    public void testFieldBlockOffsetsIsNullWhenThereIsNoNullRow()
    {
        Block fieldBlock = new ByteArrayBlock(1, Optional.empty(), new byte[]{10});
        AbstractRowBlock rowBlock = (RowBlock) RowBlock.fromFieldBlocks(1, Optional.empty(), new Block[] {fieldBlock});
        // Blocks should discard the offset mask during creation if no values are null
        assertNull(rowBlock.getFieldBlockOffsets());
    }

    @Test
    public void testFieldBlockOffsetsIsNotNullWhenThereIsNullRow()
    {
        Block fieldBlock = new ByteArrayBlock(1, Optional.empty(), new byte[]{10});
        AbstractRowBlock rowBlock = (RowBlock) RowBlock.fromFieldBlocks(1, Optional.of(new boolean[] {true}), new Block[] {fieldBlock});
        // Blocks should not discard the offset mask during creation if no values are null
        assertNotNull(rowBlock.getFieldBlockOffsets());
    }
}
