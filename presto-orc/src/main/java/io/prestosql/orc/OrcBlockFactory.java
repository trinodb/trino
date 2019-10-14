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
package io.prestosql.orc;

import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LazyBlock;
import io.prestosql.spi.block.LazyBlockLoader;

import java.io.IOException;
import java.util.function.Consumer;
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

    public Block createBlock(int positionCount, OrcBlockReader reader, Consumer<Block> onBlockLoaded)
    {
        return new LazyBlock(positionCount, new OrcBlockLoader(reader, onBlockLoaded));
    }

    public NestedBlockFactory createNestedBlockFactory(Consumer<Block> onBlockLoaded)
    {
        return new NestedBlockFactory(nestedLazy, onBlockLoaded);
    }

    public interface OrcBlockReader
    {
        Block readBlock()
                throws IOException;
    }

    public class NestedBlockFactory
    {
        private final boolean lazy;
        private final Consumer<Block> onBlockLoaded;

        private NestedBlockFactory(boolean lazy, Consumer<Block> onBlockLoaded)
        {
            this.lazy = lazy;
            this.onBlockLoaded = requireNonNull(onBlockLoaded, "onBlockLoaded is null");
        }

        public Block createBlock(int positionCount, OrcBlockReader reader)
        {
            if (lazy) {
                return new LazyBlock(positionCount, new OrcBlockLoader(reader, onBlockLoaded));
            }

            try {
                Block block = reader.readBlock();
                onBlockLoaded.accept(block);
                return block;
            }
            catch (Exception e) {
                throw exceptionTransform.apply(e);
            }
        }
    }

    private final class OrcBlockLoader
            implements LazyBlockLoader
    {
        private final int expectedPageId = currentPageId;
        private final OrcBlockReader blockReader;
        private final Consumer<Block> onBlockLoaded;
        private boolean loaded;

        public OrcBlockLoader(OrcBlockReader blockReader, Consumer<Block> onBlockLoaded)
        {
            this.blockReader = requireNonNull(blockReader, "blockReader is null");
            this.onBlockLoaded = requireNonNull(onBlockLoaded, "onBlockLoaded is null");
        }

        @Override
        public final Block load()
        {
            checkState(!loaded, "Already loaded");
            checkState(currentPageId == expectedPageId, "ORC reader has been advanced beyond block");

            Block block;
            try {
                block = blockReader.readBlock();
                onBlockLoaded.accept(block);
            }
            catch (IOException | RuntimeException e) {
                throw exceptionTransform.apply(e);
            }

            loaded = true;
            return block;
        }
    }
}
