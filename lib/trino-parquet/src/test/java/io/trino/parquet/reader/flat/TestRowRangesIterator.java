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
package io.trino.parquet.reader.flat;

import com.google.common.base.VerifyException;
import io.trino.parquet.reader.FilteredRowRanges;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static io.trino.parquet.reader.FilteredRowRanges.RowRange;
import static io.trino.parquet.reader.TestingRowRanges.toRowRange;
import static io.trino.parquet.reader.TestingRowRanges.toRowRanges;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRowRangesIterator
{
    @Test
    public void testNullRowRanges()
    {
        RowRangesIterator ranges = RowRangesIterator.createRowRangesIterator(Optional.empty());
        ranges.resetForNewPage(OptionalLong.empty());
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);
        assertThat(ranges.advanceRange(100)).isEqualTo(100);
        assertThat(ranges.seekForward(100)).isEqualTo(100);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(Integer.MAX_VALUE);
        assertThat(ranges.isPageFullyConsumed(100)).isTrue();
    }

    @Test
    public void testRowRangesWithoutFirstPageIndex()
    {
        RowRangesIterator ranges = RowRangesIterator.createRowRangesIterator(Optional.of(new FilteredRowRanges(toRowRange(50))));
        assertThatThrownBy(() -> ranges.resetForNewPage(OptionalLong.empty()))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSkipToRangeStart()
    {
        RowRangesIterator ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.skipToRangeStart()).isEqualTo(20);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(11);
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(10));
        assertThat(ranges.skipToRangeStart()).isEqualTo(10);
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(25));
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(35));
        assertThat(ranges.skipToRangeStart()).isEqualTo(15);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(50);
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(75));
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(25);
        assertThat(ranges.skipToRangeStart()).isEqualTo(0);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(25);

        assertThatThrownBy(() -> createRowRangesIterator(range(20, 30), range(50, 99))
                .resetForNewPage(OptionalLong.of(100)))
                .isInstanceOf(VerifyException.class);
    }

    @Test
    public void testIsPageFullyConsumed()
    {
        RowRangesIterator ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.isPageFullyConsumed(5)).isFalse();
        assertThat(ranges.isPageFullyConsumed(31)).isFalse();

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(20));
        assertThat(ranges.isPageFullyConsumed(11)).isTrue();
        assertThat(ranges.isPageFullyConsumed(12)).isFalse();

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(25));
        assertThat(ranges.isPageFullyConsumed(6)).isTrue();
        assertThat(ranges.isPageFullyConsumed(7)).isFalse();
    }

    @Test
    public void testAdvanceRange()
    {
        RowRangesIterator ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.skipToRangeStart()).isEqualTo(20);
        assertThat(ranges.advanceRange(10)).isEqualTo(10);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(1);
        assertThat(ranges.advanceRange(15)).isEqualTo(1);
        assertThat(ranges.skipToRangeStart()).isEqualTo(19);
        assertThat(ranges.getRowsLeftInCurrentRange()).isEqualTo(50);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(20));
        assertThat(ranges.advanceRange(11)).isEqualTo(11);
        assertThat(ranges.skipToRangeStart()).isEqualTo(19);
        assertThat(ranges.advanceRange(10)).isEqualTo(10);
        assertThat(ranges.advanceRange(40)).isEqualTo(40);
        RowRangesIterator consumedRange = ranges;
        assertThatThrownBy(() -> consumedRange.advanceRange(1))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testSeekForward()
    {
        RowRangesIterator ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(5));
        assertThat(ranges.seekForward(95)).isEqualTo(61);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.seekForward(90)).isEqualTo(51);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.seekForward(90)).isEqualTo(51);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(0));
        assertThat(ranges.seekForward(40)).isEqualTo(11);

        ranges = createRowRangesIterator(range(20, 30), range(50, 99));
        ranges.resetForNewPage(OptionalLong.of(10));
        assertThat(ranges.seekForward(5)).isEqualTo(0);
        assertThat(ranges.seekForward(5)).isEqualTo(0);
        assertThat(ranges.seekForward(5)).isEqualTo(5);
        assertThat(ranges.seekForward(5)).isEqualTo(5);
        assertThat(ranges.seekForward(5)).isEqualTo(1);
        assertThat(ranges.seekForward(65)).isEqualTo(50);
        RowRangesIterator consumedRange = ranges;
        assertThatThrownBy(() -> consumedRange.seekForward(1))
                .isInstanceOf(IllegalStateException.class);
    }

    private static RowRangesIterator createRowRangesIterator(RowRange... ranges)
    {
        return RowRangesIterator.createRowRangesIterator(Optional.of(new FilteredRowRanges(toRowRanges(ranges))));
    }

    private static RowRange range(long start, long end)
    {
        return new RowRange(start, end);
    }
}
