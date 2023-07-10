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
package io.trino.parquet.writer;

import com.google.common.collect.ImmutableList;
import io.trino.parquet.writer.repdef.DefLevelWriterProvider;
import io.trino.parquet.writer.repdef.RepLevelWriterProvider;
import io.trino.spi.block.Block;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class ColumnChunk
{
    private final Block block;
    private final List<DefLevelWriterProvider> defLevelWriterProviders;
    private final List<RepLevelWriterProvider> repLevelWriterProviders;

    ColumnChunk(Block block)
    {
        this(block, ImmutableList.of(), ImmutableList.of());
    }

    ColumnChunk(Block block, List<DefLevelWriterProvider> defLevelWriterProviders, List<RepLevelWriterProvider> repLevelWriterProviders)
    {
        this.block = requireNonNull(block, "block is null");
        this.defLevelWriterProviders = ImmutableList.copyOf(defLevelWriterProviders);
        this.repLevelWriterProviders = ImmutableList.copyOf(repLevelWriterProviders);
    }

    List<DefLevelWriterProvider> getDefLevelWriterProviders()
    {
        return defLevelWriterProviders;
    }

    public List<RepLevelWriterProvider> getRepLevelWriterProviders()
    {
        return repLevelWriterProviders;
    }

    public Block getBlock()
    {
        return block;
    }
}
