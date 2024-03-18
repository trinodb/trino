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
package io.trino.plugin.varada.dispatcher;

import io.trino.plugin.varada.storage.read.VaradaStoragePageSource;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorPageSource;

import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;

public class TestingVaradaPageSource
        implements VaradaStoragePageSource
{
    private final Iterator<DispatcherPageSourceTest.TestPage> testPages;
    private long rowsLimit;
    private ConnectorPageSource.RowRanges currentRowRange;
    private boolean closed;

    public TestingVaradaPageSource(List<DispatcherPageSourceTest.TestPage> testPages)
    {
        this.rowsLimit = testPages.stream().map(DispatcherPageSourceTest.TestPage::getPage).mapToInt(Page::getPositionCount).sum();
        this.testPages = testPages.iterator();
    }

    @Override
    public Page getNextPage()
    {
        checkState(!closed);
        if (!testPages.hasNext()) {
            checkState(rowsLimit == 0);
            currentRowRange = ConnectorPageSource.RowRanges.EMPTY;
            return new Page(0);
        }
        DispatcherPageSourceTest.TestPage testPage = testPages.next();
        rowsLimit -= testPage.getPage().getPositionCount();
        currentRowRange = testPage.getMatchedRanges();
        return testPage.getPage();
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public boolean isFinished()
    {
        return !testPages.hasNext();
    }

    @Override
    public long getCompletedPositions()
    {
        return 0;
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public void close()
    {
        closed = true;
    }

    @Override
    public ConnectorPageSource.RowRanges getSortedRowRanges()
    {
        return currentRowRange;
    }

    @Override
    public boolean isRowsLimitReached()
    {
        return rowsLimit <= 0;
    }
}
