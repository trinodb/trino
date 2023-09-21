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

import com.google.inject.Inject;
import io.trino.spi.Page;
import io.trino.spi.PageSorter;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.type.Type;

import java.util.List;

import static io.trino.operator.SyntheticAddress.decodePosition;
import static io.trino.operator.SyntheticAddress.decodeSliceIndex;
import static java.util.Objects.requireNonNull;

public class PagesIndexPageSorter
        implements PageSorter
{
    private final PagesIndex.Factory pagesIndexFactory;

    @Inject
    public PagesIndexPageSorter(PagesIndex.Factory pagesIndexFactory)
    {
        this.pagesIndexFactory = requireNonNull(pagesIndexFactory, "pagesIndexFactory is null");
    }

    @Override
    public long[] sort(List<Type> types, List<Page> pages, List<Integer> sortChannels, List<SortOrder> sortOrders, int expectedPositions)
    {
        PagesIndex pagesIndex = pagesIndexFactory.newPagesIndex(types, expectedPositions);
        pages.forEach(pagesIndex::addPage);
        pagesIndex.sort(sortChannels, sortOrders);

        return pagesIndex.getValueAddresses().toLongArray();
    }

    @Override
    public int decodePageIndex(long address)
    {
        return decodeSliceIndex(address);
    }

    @Override
    public int decodePositionIndex(long address)
    {
        return decodePosition(address);
    }
}
