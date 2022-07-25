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

import io.trino.block.BlockAssertions;
import io.trino.operator.aggregation.GroupedAggregator;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;

import java.util.function.BiConsumer;

import static java.lang.String.format;
import static org.testng.Assert.assertEquals;

public class AggregationTestOutput
{
    private final Object expectedValue;

    public AggregationTestOutput(Object expectedValue)
    {
        this.expectedValue = expectedValue;
    }

    public void validateAggregator(Type finalType, GroupedAggregator groupedAggregator, long groupId)
    {
        createEqualAssertion(expectedValue, groupId).accept(getGroupValue(finalType, groupedAggregator, (int) groupId), expectedValue);
    }

    private static BiConsumer<Object, Object> createEqualAssertion(Object expectedValue, long groupId)
    {
        BiConsumer<Object, Object> equalAssertion = (actual, expected) -> assertEquals(actual, expected, format("failure on group %s", groupId));

        if (expectedValue instanceof Double && !expectedValue.equals(Double.NaN)) {
            equalAssertion = (actual, expected) -> assertEquals((double) actual, (double) expected, 1e-10);
        }
        if (expectedValue instanceof Float && !expectedValue.equals(Float.NaN)) {
            equalAssertion = (actual, expected) -> assertEquals((float) actual, (float) expected, 1e-10f);
        }
        return equalAssertion;
    }

    private static Object getGroupValue(Type finalType, GroupedAggregator groupedAggregator, int groupId)
    {
        BlockBuilder out = finalType.createBlockBuilder(null, 1);
        groupedAggregator.evaluate(groupId, out);
        return BlockAssertions.getOnlyValue(finalType, out.build());
    }
}
