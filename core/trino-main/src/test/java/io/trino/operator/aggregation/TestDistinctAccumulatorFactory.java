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
package io.trino.operator.aggregation;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.ExceededMemoryLimitException;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.sql.tree.QualifiedName;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.OptionalInt;

import static io.trino.ExceededMemoryLimitException.exceededLocalTotalMemoryLimit;
import static io.trino.block.BlockAssertions.createBooleansBlock;
import static io.trino.block.BlockAssertions.createLongsBlock;
import static io.trino.operator.aggregation.AggregationTestUtils.createGroupByIdBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestDistinctAccumulatorFactory
{
    private static final TestingFunctionResolution testingFunctionResolution = new TestingFunctionResolution();
    private static final TestingAggregationFunction function = testingFunctionResolution.getAggregateFunction(QualifiedName.of("sum"), fromTypes(BIGINT));
    private static final AggregatorFactory distinctFactory = function.createDistinctAggregatorFactory(SINGLE, ImmutableList.of(0), OptionalInt.of(1));
    private static final Long[] longBlock = new Long[20000];
    private static final Boolean[] booleanBlock = new Boolean[20000];

    @BeforeClass
    public void setUp()
    {
        for (int i = 0; i < longBlock.length; i++) {
            longBlock[i] = (long) i;
        }
        Arrays.fill(booleanBlock, true);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class)
    public void testUpdateMemory()
    {
        Page page = new Page(createLongsBlock(longBlock), createBooleansBlock(booleanBlock));
        Aggregator aggregator = distinctFactory.createAggregator(() -> {
            throw exceededLocalTotalMemoryLimit(DataSize.ofBytes(0), "Test succeeds");
        });
        aggregator.processPage(page);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class)
    public void testUpdateMemoryForGroupedDistinctAccumulator()
    {
        Page page = new Page(createLongsBlock(longBlock), createBooleansBlock(booleanBlock));
        GroupedAggregator aggregator = distinctFactory.createGroupedAggregator(() -> {
            throw exceededLocalTotalMemoryLimit(DataSize.ofBytes(0), "Test succeeds");
        });
        aggregator.processPage(createGroupByIdBlock(0, page.getPositionCount()), page);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class)
    public void testUpdateMemoryForUnspillingGroupedDistinctAccumulator()
    {
        Page page = new Page(createLongsBlock(longBlock), createBooleansBlock(booleanBlock));
        GroupedAggregator aggregator = distinctFactory.createUnspillGroupedAggregator(SINGLE, 0, () -> {
            throw exceededLocalTotalMemoryLimit(DataSize.ofBytes(0), "Test succeeds");
        });
        aggregator.processPage(createGroupByIdBlock(0, page.getPositionCount()), page);
    }
}
