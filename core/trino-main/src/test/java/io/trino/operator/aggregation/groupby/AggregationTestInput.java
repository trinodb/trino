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

package io.trino.operator.aggregation.groupby;

import com.google.common.primitives.Ints;
import io.trino.operator.aggregation.AggregationTestUtils;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.operator.aggregation.TestingAggregationFunction;
import io.trino.spi.Page;
import io.trino.spi.type.Type;

import java.util.OptionalInt;
import java.util.stream.IntStream;

import static io.trino.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class AggregationTestInput
{
    private final Page[] pages;
    private final TestingAggregationFunction function;
    private final int[] args;

    private final int offset;

    public AggregationTestInput(TestingAggregationFunction function, Page[] pages, int offset)
    {
        this.pages = pages;
        this.function = function;
        args = IntStream.range(0, pages[0].getChannelCount()).toArray();
        this.offset = offset;
    }

    public void runPagesOnAggregatorWithAssertion(int groupId, Type finalType, GroupedAggregator groupedAggregator, AggregationTestOutput expectedValue)
    {
        for (Page page : getPages()) {
            groupedAggregator.processPage(groupId, createGroupByIdBlock(groupId, page.getPositionCount()), page);
        }

        expectedValue.validateAggregator(finalType, groupedAggregator, groupId);
    }

    private Page[] getPages()
    {
        Page[] pages = this.pages;

        if (offset > 0) {
            pages = AggregationTestUtils.offsetColumns(pages, offset);
        }

        return pages;
    }

    public GroupedAggregator createGroupedAggregator()
    {
        return function.createAggregatorFactory(SINGLE, Ints.asList(args), OptionalInt.empty())
                .createGroupedAggregator();
    }
}
