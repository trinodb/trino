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
package io.trino.operator;

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.ValueBlock;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Fills sort key prefixes with the type's batch sort key prefix operator when the page's sort
 * channel is a value block without nulls, falling back to the per-position filler otherwise. The
 * batch operator produces raw prefixes, which are then adjusted for the layout (descending order
 * and bit alignment) in a separate pass over the array. Only single-channel layouts are supported,
 * since the batch operator overwrites the prefix array instead of merging into it.
 */
public class BatchPageSortKeyPrefixFiller
        implements PageSortKeyPrefixFiller
{
    private final MethodHandle batchOperator;
    private final PageSortKeyPrefixFiller fallback;
    private final int sortChannel;
    private final boolean descending;
    private final int extractShift;
    private final boolean identityLayout;

    public BatchPageSortKeyPrefixFiller(MethodHandle batchOperator, SortKeyPrefixFiller.SortKeyPrefixLayout layout, PageSortKeyPrefixFiller fallback)
    {
        this.batchOperator = requireNonNull(batchOperator, "batchOperator is null");
        this.fallback = requireNonNull(fallback, "fallback is null");
        if (layout.hasNullBit() || layout.shift() != 0 || layout.valueBase() != 0) {
            throw new IllegalArgumentException("Batch filling requires a single-channel layout");
        }
        this.sortChannel = layout.sortChannel();
        this.descending = layout.descending();
        this.extractShift = layout.extractShift();
        this.identityLayout = !descending && extractShift == 0;
    }

    @Override
    public void fill(Page page, long[] prefixes)
    {
        Block block = page.getBlock(sortChannel);
        int positionCount = page.getPositionCount();
        if (!(block instanceof ValueBlock valueBlock) || block.mayHaveNull()) {
            fallback.fill(page, prefixes);
            return;
        }
        try {
            batchOperator.invokeExact(valueBlock, prefixes, positionCount);
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
        if (!identityLayout) {
            for (int i = 0; i < positionCount; i++) {
                long encoded = prefixes[i];
                if (descending) {
                    encoded = ~encoded;
                }
                prefixes[i] = encoded >>> extractShift;
            }
        }
    }
}
