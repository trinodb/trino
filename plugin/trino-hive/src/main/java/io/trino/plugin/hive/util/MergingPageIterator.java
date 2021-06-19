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
package io.trino.plugin.hive.util;

import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;

import java.lang.invoke.MethodHandle;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterators.concat;
import static com.google.common.collect.Iterators.mergeSorted;
import static com.google.common.collect.Iterators.transform;
import static io.trino.plugin.hive.util.SortBuffer.appendPositionTo;
import static io.trino.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static java.util.Comparator.naturalOrder;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class MergingPageIterator
        extends AbstractIterator<Page>
{
    private final List<Integer> sortFields;
    private final List<MethodHandle> orderingOperators;
    private final PageBuilder pageBuilder;
    private final Iterator<PagePosition> pagePositions;

    public MergingPageIterator(
            Collection<Iterator<Page>> iterators,
            List<Type> types,
            List<Integer> sortFields,
            List<SortOrder> sortOrders,
            TypeOperators typeOperators)
    {
        requireNonNull(sortFields, "sortFields is null");
        requireNonNull(sortOrders, "sortOrders is null");
        checkArgument(sortFields.size() == sortOrders.size(), "sortFields and sortOrders size must match");

        this.sortFields = ImmutableList.copyOf(sortFields);

        ImmutableList.Builder<MethodHandle> orderingOperators = ImmutableList.builder();
        for (int index = 0; index < sortFields.size(); index++) {
            Type type = types.get(sortFields.get(index));
            SortOrder sortOrder = sortOrders.get(index);
            orderingOperators.add(typeOperators.getOrderingOperator(type, sortOrder, simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION)));
        }
        this.orderingOperators = orderingOperators.build();

        this.pageBuilder = new PageBuilder(types);
        this.pagePositions = mergeSorted(
                iterators.stream()
                        .map(pages -> concat(transform(pages, PagePositionIterator::new)))
                        .collect(toList()),
                naturalOrder());
    }

    @Override
    protected Page computeNext()
    {
        while (!pageBuilder.isFull() && pagePositions.hasNext()) {
            pagePositions.next().appendTo(pageBuilder);
        }

        if (pageBuilder.isEmpty()) {
            return endOfData();
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();
        return page;
    }

    private class PagePositionIterator
            extends AbstractIterator<PagePosition>
    {
        private final Page page;
        private int position = -1;

        private PagePositionIterator(Page page)
        {
            this.page = requireNonNull(page, "page is null");
        }

        @Override
        protected PagePosition computeNext()
        {
            position++;
            if (position == page.getPositionCount()) {
                return endOfData();
            }
            return new PagePosition(page, position);
        }
    }

    @SuppressWarnings("ComparableImplementedButEqualsNotOverridden")
    private class PagePosition
            implements Comparable<PagePosition>
    {
        private final Page page;
        private final int position;

        public PagePosition(Page page, int position)
        {
            this.page = requireNonNull(page, "page is null");
            this.position = position;
        }

        public void appendTo(PageBuilder pageBuilder)
        {
            appendPositionTo(page, position, pageBuilder);
        }

        @Override
        public int compareTo(PagePosition other)
        {
            try {
                for (int i = 0; i < sortFields.size(); i++) {
                    int channel = sortFields.get(i);
                    MethodHandle orderingOperator = orderingOperators.get(i);

                    Block block = page.getBlock(channel);
                    Block otherBlock = other.page.getBlock(channel);

                    int result = (int) orderingOperator.invokeExact(block, position, otherBlock, other.position);
                    if (result != 0) {
                        return result;
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
}
