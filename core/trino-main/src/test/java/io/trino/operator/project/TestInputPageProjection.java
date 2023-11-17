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
package io.trino.operator.project;

import io.trino.operator.DriverYieldSignal;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import org.junit.jupiter.api.Test;

import static io.trino.block.BlockAssertions.createLongSequenceBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;

public class TestInputPageProjection
{
    @Test
    public void testLazyInputPage()
    {
        InputPageProjection projection = new InputPageProjection(0, BIGINT);
        Block block = createLongSequenceBlock(0, 100);
        Block result = projection.project(SESSION, new DriverYieldSignal(), new Page(block), SelectedPositions.positionsRange(0, 100)).getResult();
        assertThat(result instanceof LazyBlock).isFalse();

        block = lazyWrapper(block);
        result = projection.project(SESSION, new DriverYieldSignal(), new Page(block), SelectedPositions.positionsRange(0, 100)).getResult();
        assertThat(result instanceof LazyBlock).isTrue();
        assertThat(result.isLoaded()).isFalse();
    }

    private static LazyBlock lazyWrapper(Block block)
    {
        return new LazyBlock(block.getPositionCount(), block::getLoadedBlock);
    }
}
