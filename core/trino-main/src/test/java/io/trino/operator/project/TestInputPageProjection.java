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
package io.prestosql.operator.project;

import io.prestosql.operator.DriverYieldSignal;
import io.prestosql.spi.Page;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import org.testng.annotations.Test;

import static io.prestosql.block.BlockAssertions.createLongSequenceBlock;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.testing.TestingConnectorSession.SESSION;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestInputPageProjection
{
    @Test
    public void testLazyInputPage()
    {
        InputPageProjection projection = new InputPageProjection(0, BIGINT);
        Block block = createLongSequenceBlock(0, 100);
        Block result = projection.project(SESSION, new DriverYieldSignal(), new Page(block), SelectedPositions.positionsRange(0, 100)).getResult();
        assertFalse(result instanceof LazyBlock);

        block = lazyWrapper(block);
        result = projection.project(SESSION, new DriverYieldSignal(), new Page(block), SelectedPositions.positionsRange(0, 100)).getResult();
        assertTrue(result instanceof LazyBlock);
        assertFalse(result.isLoaded());
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }
}
