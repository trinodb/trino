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
package io.trino.plugin.iceberg.delete;

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.apache.iceberg.StructLike;

import java.util.Iterator;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.plugin.iceberg.IcebergPageSink.getIcebergValue;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

public class TrinoRow
        implements StructLike
{
    private final Type[] types;
    private final Block[] blocks;
    private final int position;

    public TrinoRow(Type[] types, Block[] blocks, int position)
    {
        this.types = requireNonNull(types, "types list is null");
        this.blocks = requireNonNull(blocks, "blocks array is null");
        checkArgument(position >= 0, "page position must be non-negative: %s", position);
        this.position = position;
    }

    public int getPosition()
    {
        return position;
    }

    @Override
    public int size()
    {
        return blocks.length;
    }

    @Override
    public <T> T get(int i, Class<T> aClass)
    {
        return aClass.cast(getIcebergValue(blocks[i], position, types[i]));
    }

    @Override
    public <T> void set(int i, T t)
    {
        throw new TrinoException(NOT_SUPPORTED, "writing to TrinoRow is not supported");
    }

    public static Iterable<TrinoRow> fromBlocks(Type[] types, Block[] blocks, int batchSize)
    {
        return new Iterable<TrinoRow>()
        {
            private int i;

            @Override
            public Iterator<TrinoRow> iterator()
            {
                return new Iterator<TrinoRow>() {
                    @Override
                    public boolean hasNext()
                    {
                        return i < batchSize;
                    }

                    @Override
                    public TrinoRow next()
                    {
                        return new TrinoRow(types, blocks, i++);
                    }
                };
            }
        };
    }
}
