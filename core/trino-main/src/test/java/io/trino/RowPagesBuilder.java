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
package io.trino;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.RowPageBuilder.rowPageBuilder;
import static java.util.Objects.requireNonNull;

public class RowPagesBuilder
{
    public static RowPagesBuilder rowPagesBuilder(Type... types)
    {
        return rowPagesBuilder(ImmutableList.copyOf(types));
    }

    public static RowPagesBuilder rowPagesBuilder(Iterable<Type> types)
    {
        return new RowPagesBuilder(types);
    }

    public static RowPagesBuilder rowPagesBuilder(List<Integer> hashChannels, Type... types)
    {
        return rowPagesBuilder(hashChannels, ImmutableList.copyOf(types));
    }

    public static RowPagesBuilder rowPagesBuilder(List<Integer> hashChannels, Iterable<Type> types)
    {
        return new RowPagesBuilder(Optional.of(hashChannels), types);
    }

    private final ImmutableList.Builder<Page> pages = ImmutableList.builder();
    private final List<Type> types;
    private RowPageBuilder builder;
    private final Optional<List<Integer>> hashChannels;

    RowPagesBuilder(Iterable<Type> types)
    {
        this(Optional.empty(), types);
    }

    RowPagesBuilder(Optional<List<Integer>> hashChannels, Iterable<Type> types)
    {
        this.types = ImmutableList.copyOf(requireNonNull(types, "types is null"));
        this.hashChannels = hashChannels.map(ImmutableList::copyOf);
        builder = rowPageBuilder(types);
    }

    public RowPagesBuilder addSequencePage(int length, int... initialValues)
    {
        checkArgument(length > 0, "length must be at least 1");
        requireNonNull(initialValues, "initialValues is null");
        checkArgument(initialValues.length == types.size(), "Expected %s initialValues, but got %s", types.size(), initialValues.length);

        pageBreak();
        Page page = SequencePageBuilder.createSequencePage(types, length, initialValues);
        pages.add(page);
        return this;
    }

    public RowPagesBuilder addBlocksPage(Iterable<Block> blocks)
    {
        return addBlocksPage(Iterables.toArray(blocks, Block.class));
    }

    public RowPagesBuilder addBlocksPage(Block... blocks)
    {
        pages.add(new Page(blocks));
        return this;
    }

    public RowPagesBuilder row(Object... values)
    {
        builder.row(values);
        return this;
    }

    public RowPagesBuilder rows(Object[]... rows)
    {
        for (Object[] row : rows) {
            row(row);
        }
        return this;
    }

    public RowPagesBuilder pageBreak()
    {
        if (!builder.isEmpty()) {
            pages.add(builder.build());
            builder = rowPageBuilder(types);
        }
        return this;
    }

    public List<Page> build()
    {
        pageBreak();
        return pages.build();
    }

    public List<Type> getTypes()
    {
        return types;
    }

    public Optional<List<Integer>> getHashChannels()
    {
        return hashChannels;
    }
}
