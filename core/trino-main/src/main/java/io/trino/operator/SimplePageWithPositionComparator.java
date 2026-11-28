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

import com.google.common.primitives.Ints;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionComparator
        implements PageWithPositionComparator
{
    private static final InvocationConvention INVOCATION_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);

    private final int[] sortChannels;
    private final MethodHandle[] orderingOperators;
    // empty when a sort key prefix cannot be derived from the first sort channel
    private final Optional<SortKeyPrefix.Source> prefixSource;
    private final Optional<Type> prefixType;
    private final Optional<SortOrder> prefixSortOrder;

    public SimplePageWithPositionComparator(List<Type> sortTypes, List<Integer> sortChannels, List<SortOrder> sortOrders, TypeOperators typeOperators)
    {
        requireNonNull(sortChannels, "sortChannels is null");
        requireNonNull(sortTypes, "sortTypes is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortTypes.size() == sortChannels.size(), "sortTypes and sortChannels must be the same size");
        checkArgument(sortTypes.size() == sortOrders.size(), "sortTypes and sortOrders must be the same size");
        this.sortChannels = Ints.toArray(sortChannels);
        this.orderingOperators = new MethodHandle[this.sortChannels.length];
        for (int index = 0; index < this.sortChannels.length; index++) {
            Type type = sortTypes.get(index);
            SortOrder sortOrder = sortOrders.get(index);
            orderingOperators[index] = typeOperators.getOrderingOperator(type, sortOrder, INVOCATION_CONVENTION);
        }
        this.prefixType = sortTypes.stream().findFirst();
        this.prefixSortOrder = sortOrders.stream().findFirst();
        this.prefixSource = prefixType.flatMap(SortKeyPrefix::prefixSource);
    }

    @Override
    public long sortKeyPrefix(Page page, int position)
    {
        if (prefixSource.isEmpty()) {
            return 0;
        }

        SortOrder sortOrder = prefixSortOrder.orElseThrow();
        Block block = page.getBlock(sortChannels[0]);
        if (block.isNull(position)) {
            return SortKeyPrefix.nullPrefix(sortOrder);
        }
        long prefix = switch (prefixSource.get()) {
            case LONG -> prefixType.orElseThrow().getLong(block, position);
            case BYTES -> SortKeyPrefix.bytesPrefix(block, position);
        };
        if (sortOrder.isAscending()) {
            return prefix;
        }
        return SortKeyPrefix.descending(prefix);
    }

    @Override
    public int compareTo(Page left, int leftPosition, Page right, int rightPosition)
    {
        try {
            for (int i = 0; i < sortChannels.length; i++) {
                int sortChannel = sortChannels[i];
                Block leftBlock = left.getBlock(sortChannel);
                Block rightBlock = right.getBlock(sortChannel);

                MethodHandle orderingOperator = orderingOperators[i];
                int compare = (int) orderingOperator.invokeExact(leftBlock, leftPosition, rightBlock, rightPosition);
                if (compare != 0) {
                    return compare;
                }
            }
            return 0;
        }
        catch (Throwable throwable) {
            throwIfUnchecked(throwable);
            throw new TrinoException(GENERIC_INTERNAL_ERROR, throwable);
        }
    }
}
