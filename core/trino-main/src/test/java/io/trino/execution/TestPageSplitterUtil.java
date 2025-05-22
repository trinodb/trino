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
package io.trino.execution;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.RunLengthEncodedBlock;
import io.trino.spi.block.VariableWidthBlockBuilder;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.airlift.slice.Slices.wrappedBuffer;
import static io.trino.SequencePageBuilder.createSequencePage;
import static io.trino.SessionTestUtils.TEST_SESSION;
import static io.trino.execution.buffer.PageSplitterUtil.splitPage;
import static io.trino.operator.OperatorAssertion.toMaterializedResult;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPageSplitterUtil
{
    @Test
    public void testSplitPage()
    {
        int positionCount = 10;
        int maxPageSizeInBytes = 100;
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);

        Page largePage = createSequencePage(types, positionCount, 0, 1, 1);
        List<Page> pages = splitPage(largePage, maxPageSizeInBytes);

        assertThat(pages).hasSizeGreaterThan(1);
        assertPageSize(pages, maxPageSizeInBytes);
        assertPositionCount(pages, positionCount);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, pages);
        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, ImmutableList.of(largePage));
        assertThat(actual).containsExactlyElementsOf(expected);
    }

    private static void assertPageSize(List<Page> pages, long maxPageSizeInBytes)
    {
        for (Page page : pages) {
            assertThat(page.getSizeInBytes()).isLessThanOrEqualTo(maxPageSizeInBytes);
        }
    }

    private static void assertPositionCount(List<Page> pages, int positionCount)
    {
        int totalPositionCount = 0;
        for (Page page : pages) {
            totalPositionCount += page.getPositionCount();
        }
        assertThat(totalPositionCount).isEqualTo(positionCount);
    }

    @Test
    public void testSplitPageNonDecreasingPageSize()
    {
        int positionCount = 100;
        int maxPageSizeInBytes = 1;
        List<Type> types = ImmutableList.of(VARCHAR);

        Slice expectedValue = wrappedBuffer("test".getBytes(UTF_8));
        VariableWidthBlockBuilder blockBuilder = VARCHAR.createBlockBuilder(null, 1, expectedValue.length());
        blockBuilder.writeEntry(expectedValue);
        Block rleBlock = RunLengthEncodedBlock.create(blockBuilder.build(), positionCount);
        Page initialPage = new Page(rleBlock);
        List<Page> pages = splitPage(initialPage, maxPageSizeInBytes);

        // the page should only be split in half as the recursion should terminate
        // after seeing that the size of the Page doesn't decrease
        assertThat(pages).hasSize(2);
        Page first = pages.get(0);
        Page second = pages.get(1);

        // the size of the pages will remain the same and should be greater than the maxPageSizeInBytes
        assertThat((int) first.getSizeInBytes()).isGreaterThan(maxPageSizeInBytes);
        assertThat((int) second.getSizeInBytes()).isGreaterThan(maxPageSizeInBytes);
        assertPositionCount(pages, positionCount);
        MaterializedResult actual = toMaterializedResult(TEST_SESSION, types, pages);
        MaterializedResult expected = toMaterializedResult(TEST_SESSION, types, ImmutableList.of(initialPage));
        assertThat(actual).containsExactlyElementsOf(expected);
    }
}
