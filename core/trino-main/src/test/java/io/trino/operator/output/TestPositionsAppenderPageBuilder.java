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
package io.trino.operator.output;

import io.airlift.slice.Slices;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.type.BlockTypeOperators;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestPositionsAppenderPageBuilder
{
    @Test
    public void testFullOnPositionCountLimit()
    {
        int maxPageBytes = 1024 * 1024;
        PositionsAppenderPageBuilder pageBuilder = PositionsAppenderPageBuilder.withMaxPageSize(
                maxPageBytes,
                List.of(VARCHAR),
                new PositionsAppenderFactory(new BlockTypeOperators()));

        Block rleBlock = RunLengthEncodedBlock.create(VARCHAR, Slices.utf8Slice("test"), 10);
        Page inputPage = new Page(rleBlock);

        IntArrayList positions = IntArrayList.wrap(new int[]{0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        // Append 32760 positions, just less than MAX_POSITION_COUNT
        assertEquals(32768, PositionsAppenderPageBuilder.MAX_POSITION_COUNT, "expected MAX_POSITION_COUNT to be 32768");
        for (int i = 0; i < 3276; i++) {
            pageBuilder.appendToOutputPartition(inputPage, positions);
        }
        assertFalse(pageBuilder.isFull(), "pageBuilder should still not be full");
        // Append 10 more positions, crossing the threshold on position count
        pageBuilder.appendToOutputPartition(inputPage, positions);
        assertTrue(pageBuilder.isFull(), "pageBuilder should be full");
        assertEquals(rleBlock.getSizeInBytes(), pageBuilder.getSizeInBytes());
    }
}
