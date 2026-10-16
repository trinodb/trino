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
package io.trino.spi.connector;

import com.google.common.collect.ImmutableList;
import io.trino.spi.Page;
import io.trino.spi.block.LongArrayBlock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

class TestFixedPageSource
{
    private static final List<Page> PAGES = ImmutableList.of(createPage(1), createPage(100), createPage(10_000));

    @Test
    public void testReportsPagesMemoryUsageToMemoryContext()
    {
        long memoryUsage = new FixedPageSource(PAGES).getMemoryUsage();
        assertThat(memoryUsage).isEqualTo(PAGES.stream().mapToLong(Page::getRetainedSizeInBytes).sum());

        AtomicLong reportedBytes = new AtomicLong(-1);
        try (FixedPageSource pageSource = new FixedPageSource(PAGES, reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(memoryUsage);
            // the memory must not be accounted for again when getMemoryUsage is polled
            assertThat(pageSource.getMemoryUsage()).isEqualTo(0);

            int pages = 0;
            while (!pageSource.isFinished()) {
                assertThat(pageSource.getNextSourcePage()).isNotNull();
                pages++;
                assertThat(reportedBytes).hasValue(memoryUsage);
            }
            assertThat(pages).isEqualTo(PAGES.size());
        }
        // getMemoryUsage does not drop after the page source is closed, and neither does the reported usage
        assertThat(reportedBytes).hasValue(memoryUsage);
    }

    @Test
    public void testReportsSuppliedMemoryUsageToMemoryContext()
    {
        long memoryUsage = new FixedPageSource(PAGES.iterator(), 1234).getMemoryUsage();
        assertThat(memoryUsage).isEqualTo(1234);

        AtomicLong reportedBytes = new AtomicLong(-1);
        try (FixedPageSource pageSource = new FixedPageSource(PAGES.iterator(), 1234, reportedBytes::set)) {
            assertThat(reportedBytes).hasValue(memoryUsage);
            assertThat(pageSource.getMemoryUsage()).isEqualTo(0);
        }
        assertThat(reportedBytes).hasValue(memoryUsage);
    }

    private static Page createPage(int positionCount)
    {
        return new Page(new LongArrayBlock(positionCount, Optional.empty(), new long[positionCount]));
    }
}
