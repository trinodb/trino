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

import io.trino.block.BlockAssertions;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.block.BlockAssertions.assertBlockEquals;
import static org.assertj.core.api.Assertions.assertThat;

public final class PageAssertions
{
    private PageAssertions() {}

    public static void assertPageEquals(List<Type> types, Page actualPage, Page expectedPage)
    {
        assertThat(expectedPage.getChannelCount()).isEqualTo(types.size());
        assertThat(actualPage.getChannelCount()).isEqualTo(expectedPage.getChannelCount());
        assertThat(actualPage.getPositionCount()).isEqualTo(expectedPage.getPositionCount());
        for (int i = 0; i < actualPage.getChannelCount(); i++) {
            assertBlockEquals(types.get(i), actualPage.getBlock(i), expectedPage.getBlock(i));
        }
    }

    public static void assertPagesEqual(List<Type> types, List<Page> actual, List<Page> expected)
    {
        assertThat(actual.size()).as("actual pages count").isEqualTo(expected.size());
        for (int i = 0; i < actual.size(); i++) {
            assertPageEquals(types, actual.get(i), expected.get(i));
        }
    }

    public static void assertSameDataInOrder(List<Type> types, List<Page> actual, List<Page> expected)
    {
        for (Page page : actual) {
            assertThat(page.getChannelCount()).as("actual page channel count").isEqualTo(types.size());
        }
        for (Page page : expected) {
            assertThat(page.getChannelCount()).as("expected page channel count").isEqualTo(types.size());
        }
        for (int i = 0; i < types.size(); i++) {
            int column = i;
            BlockAssertions.assertSameDataInOrder(
                    types.get(column),
                    actual.stream()
                            .map(page -> page.getBlock(column))
                            .collect(toImmutableList()),
                    expected.stream()
                            .map(page -> page.getBlock(column))
                            .collect(toImmutableList()));
        }
    }
}
