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
package io.trino.orc;

import io.trino.spi.block.Block;
import io.trino.spi.block.LazyBlock;
import io.trino.spi.block.LazyBlockLoader;

import java.io.IOException;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class OrcBlockFactory
{
    private final Function<Exception, RuntimeException> exceptionTransform;
    private final boolean nestedLazy;
    private int currentPageId;

    public OrcBlockFactory(Function<Exception, RuntimeException> exceptionTransform, boolean nestedLazy)
    {
        this.exceptionTransform = requireNonNull(exceptionTransform, "exceptionTransform is null");
        this.nestedLazy = nestedLazy;
    }

    public void nextPage()
    {
        currentPageId++;
    }

    public Block createBlock(int positionCount, OrcBlockReader reader, boolean nested)
    {
        return new LazyBlock(positionCount, new OrcBlockLoader(reader, nested && !nestedLazy));
    }

    public interface OrcBlockReader
    {
        Block readBlock()
                throws IOException;
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader
    {
        private final int expectedPageId = currentPageId;
        private final OrcBlockReader blockReader;
        private final boolean loadFully;
        private boolean loaded;

        public OrcBlockLoader(OrcBlockReader blockReader, boolean loadFully)
        {
            this.blockReader = requireNonNull(blockReader, "blockReader is null");
            this.loadFully = loadFully;
        }

        @Override
        public Block load()
        {
            checkState(!loaded, "Already loaded");
            checkState(currentPageId == expectedPageId, "ORC reader has been advanced beyond block");

            loaded = true;
            try {
                Block block = blockReader.readBlock();
                if (loadFully) {
                    block = block.getLoadedBlock();
                }
                return block;
            }
            catch (IOException | RuntimeException e) {
                throw exceptionTransform.apply(e);
            }
        }
    }
}
