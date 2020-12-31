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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Objects.requireNonNull;

public class SimplePageWithPositionComparator
        implements PageWithPositionComparator
{
    private final List<Integer> sortChannels;
    private final List<MethodHandle> orderingOperators;

    public SimplePageWithPositionComparator(List<Type> types, List<Integer> sortChannels, List<SortOrder> sortOrders, TypeOperators typeOperators)
    {
        this.sortChannels = ImmutableList.copyOf(requireNonNull(sortChannels, "sortChannels is null"));
        requireNonNull(types, "types is null");
        requireNonNull(sortOrders, "sortOrders is null");
        ImmutableList.Builder<MethodHandle> orderingOperators = ImmutableList.builder();
        for (int index = 0; index < sortChannels.size(); index++) {
            Type type = types.get(sortChannels.get(index));
            SortOrder sortOrder = sortOrders.get(index);
            orderingOperators.add(typeOperators.getOrderingOperator(type, sortOrder, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)));
        }
        this.orderingOperators = orderingOperators.build();
    }

    @Override
    public int compareTo(Page left, int leftPosition, Page right, int rightPosition)
    {
        try {
            for (int i = 0; i < sortChannels.size(); i++) {
                int sortChannel = sortChannels.get(i);
                Block leftBlock = left.getBlock(sortChannel);
                Block rightBlock = right.getBlock(sortChannel);

                MethodHandle orderingOperator = orderingOperators.get(i);
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
