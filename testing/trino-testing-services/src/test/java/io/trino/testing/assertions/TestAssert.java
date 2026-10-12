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
package io.trino.testing.assertions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static io.trino.testing.assertions.Assert.assertContainsAll;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestAssert
{
    @Test
    public void testEqualsIgnoreOrder()
    {
        assertEqualsIgnoreOrder(List.of(), List.of());
        assertEqualsIgnoreOrder(List.of(1, 2, 2, 3), List.of(2, 3, 2, 1));
        assertEqualsIgnoreOrder(Arrays.asList(1, null, null), Arrays.asList(null, 1, null));
    }

    @Test
    public void testEqualsIgnoreOrderMismatch()
    {
        assertThatThrownBy(() -> assertEqualsIgnoreOrder(List.of(1, 1, 1, 2, 3), List.of(1, 2, 2, 4)))
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        Elements do not match: 3 unexpected, 2 missing (actual count: 5, expected count: 4)
                        Unexpected:
                          1 (2 times)
                          3
                        Missing:
                          2
                          4""");
    }

    @Test
    public void testEqualsIgnoreOrderOnlyMissing()
    {
        assertThatThrownBy(() -> assertEqualsIgnoreOrder(List.of("a"), List.of("a", "a"), "Letters"))
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        Letters do not match: 0 unexpected, 1 missing (actual count: 1, expected count: 2)
                        Missing:
                          a""");
    }

    @Test
    public void testEqualsIgnoreOrderLimitsReportedElements()
    {
        assertThatThrownBy(() -> assertEqualsIgnoreOrder(range(0, 1000), range(1000, 2000)))
                .isInstanceOf(AssertionError.class)
                .hasMessageStartingWith(
                        """
                        Elements do not match: 1000 unexpected, 1000 missing (actual count: 1000, expected count: 1000)
                        Unexpected (first 100 of 1000 distinct):
                          0
                          1
                        """)
                .hasMessageContaining(
                        """
                          99
                        Missing (first 100 of 1000 distinct):
                          1000
                        """)
                .hasMessageNotContaining("\n  100\n")
                .hasMessageEndingWith("\n  1099");
    }

    @Test
    @Timeout(10)
    public void testEqualsIgnoreOrderLarge()
    {
        assertEqualsIgnoreOrder(range(0, 1_000_000), range(0, 1_000_000).reversed());
    }

    @Test
    public void testContainsAll()
    {
        assertContainsAll(List.of(), List.of());
        assertContainsAll(List.of(1, 2, 3), List.of());
        assertContainsAll(List.of(1, 2, 3), List.of(3, 1));
        assertContainsAll(List.of(1), List.of(1, 1));
        assertContainsAll(Arrays.asList(1, null), Arrays.asList(null, 1));
    }

    @Test
    public void testContainsAllMismatch()
    {
        assertThatThrownBy(() -> assertContainsAll(List.of(1, 2), List.of(3, 1, 4, 3), "Numbers"))
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        Numbers do not contain all expected: 3 missing (actual count: 2, expected count: 4)
                        Missing:
                          3 (2 times)
                          4
                        Actual:
                          1
                          2""");
    }

    @Test
    public void testContainsAllMismatchSingleUseIterable()
    {
        Iterable<Integer> actual = Stream.of(1, 2)::iterator;
        assertThatThrownBy(() -> assertContainsAll(actual, List.of(3)))
                .isInstanceOf(AssertionError.class)
                .hasMessage(
                        """
                        Elements do not contain all expected: 1 missing (actual count: 2, expected count: 1)
                        Missing:
                          3
                        Actual:
                          1
                          2""");
    }

    @Test
    @Timeout(10)
    public void testContainsAllLarge()
    {
        assertContainsAll(range(0, 1_000_000), range(0, 1_000_000).reversed());
    }

    private static List<Integer> range(int startInclusive, int endExclusive)
    {
        return IntStream.range(startInclusive, endExclusive).boxed().toList();
    }
}
