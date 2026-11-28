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

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static java.util.Objects.requireNonNull;

public class InterpretedPageSortKeyPrefixFiller
        implements PageSortKeyPrefixFiller
{
    private final MethodHandle operator;
    private final int sortChannel;
    private final boolean descending;
    private final long nullSubKey;
    private final long valueBase;
    private final int extractShift;
    private final int shift;

    public InterpretedPageSortKeyPrefixFiller(MethodHandle operator, SortKeyPrefixFiller.SortKeyPrefixLayout layout)
    {
        this.operator = requireNonNull(operator, "operator is null");
        this.sortChannel = layout.sortChannel();
        this.descending = layout.descending();
        this.nullSubKey = layout.nullSubKey();
        this.valueBase = layout.valueBase();
        this.extractShift = layout.extractShift();
        this.shift = layout.shift();
    }

    @Override
    public void fill(Page page, long[] prefixes)
    {
        Block block = page.getBlock(sortChannel);
        try {
            for (int position = 0; position < page.getPositionCount(); position++) {
                long subKey;
                if (block.isNull(position)) {
                    subKey = nullSubKey;
                }
                else {
                    long encoded = (long) operator.invokeExact(block, position);
                    if (descending) {
                        encoded = ~encoded;
                    }
                    subKey = valueBase | (encoded >>> extractShift);
                }
                prefixes[position] |= subKey << shift;
            }
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }
}
