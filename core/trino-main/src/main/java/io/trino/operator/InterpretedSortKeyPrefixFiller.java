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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class InterpretedSortKeyPrefixFiller
        implements SortKeyPrefixFiller
{
    private final MethodHandle operator;
    private final int sortChannel;
    private final boolean descending;
    private final boolean collideNullsWithValues;
    private final long nullSubKey;
    private final long valueBase;
    private final int extractShift;
    private final int shift;

    public InterpretedSortKeyPrefixFiller(MethodHandle operator, SortKeyPrefixLayout layout)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.sortChannel = layout.sortChannel();
        this.descending = layout.descending();
        this.collideNullsWithValues = layout.collideNullsWithValues();
        this.nullSubKey = layout.nullSubKey();
        this.valueBase = layout.valueBase();
        this.extractShift = layout.extractShift();
        this.shift = layout.shift();
    }

    @Override
    public boolean fill(PagesIndex pagesIndex, int startPosition, int endPosition, long[] keys, int keysOffset)
    {
        LongArrayList valueAddresses = pagesIndex.getValueAddresses();
        ObjectArrayList<Block> blocks = pagesIndex.getChannel(sortChannel);
        boolean collidingNulls = false;

        try {
            for (int position = startPosition; position < endPosition; position++) {
                long pageAddress = valueAddresses.getLong(position);
                Block block = blocks.get(decodeSliceIndex(pageAddress));
                int blockPosition = decodePosition(pageAddress);

                long subKey;
                if (block.isNull(blockPosition)) {
                    subKey = nullSubKey;
                    collidingNulls = collideNullsWithValues;
                }
                else {
                    long encoded = (long) operator.invokeExact(block, blockPosition);
                    if (descending) {
                        encoded = ~encoded;
                    }
                    subKey = valueBase | (encoded >>> extractShift);
                }
                keys[keysOffset + position - startPosition] |= subKey << shift;
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
        return collidingNulls;
    }
}
