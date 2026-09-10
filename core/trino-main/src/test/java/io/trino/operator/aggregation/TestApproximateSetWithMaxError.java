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
import io.airlift.slice.Slices;
import io.airlift.stats.cardinality.HyperLogLog;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.Page;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.SqlVarbinary;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestApproximateSetWithMaxError
{
    private static final TestingFunctionResolution FUNCTION_RESOLUTION = new TestingFunctionResolution();

    private static final double LOWEST_MAX_STANDARD_ERROR = 0.0040625;
    private static final double HIGHEST_MAX_STANDARD_ERROR = 0.26;

    @Test
    public void testCustomMaxStandardErrorProducesAccurateEstimate()
    {
        int uniques = 20000;
        HyperLogLog hll = approxSet(sequence(uniques), LOWEST_MAX_STANDARD_ERROR);

        double relativeError = Math.abs(hll.cardinality() - uniques) / (double) uniques;
        // The finest permitted precision targets ~0.4% standard error; allow generous slack to avoid flakiness.
        assertThat(relativeError).isLessThan(0.02);
    }

    @Test
    public void testSmallerErrorProducesLargerSketch()
    {
        List<Long> values = sequence(20000);

        HyperLogLog preciseSketch = approxSet(values, LOWEST_MAX_STANDARD_ERROR);
        HyperLogLog coarseSketch = approxSet(values, HIGHEST_MAX_STANDARD_ERROR);

        // A smaller maxStandardError allocates more buckets, so the serialized sketch is larger.
        assertThat(preciseSketch.serialize().length()).isGreaterThan(coarseSketch.serialize().length());
    }

    @Test
    public void testMaxStandardErrorOutOfRangeFailsQuery()
    {
        assertTrinoExceptionThrownBy(() -> approxSet(ImmutableList.of(1L), LOWEST_MAX_STANDARD_ERROR - 0.0001))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
        assertTrinoExceptionThrownBy(() -> approxSet(ImmutableList.of(1L), HIGHEST_MAX_STANDARD_ERROR + 0.0001))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testSketchesWithDifferentErrorCannotBeMerged()
    {
        List<Long> values = sequence(20000);

        HyperLogLog preciseSketch = approxSet(values, LOWEST_MAX_STANDARD_ERROR);
        HyperLogLog coarseSketch = approxSet(values, HIGHEST_MAX_STANDARD_ERROR);

        assertThatThrownBy(() -> preciseSketch.mergeWith(coarseSketch))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot merge HLLs with different number of buckets");
    }

    private static HyperLogLog approxSet(List<Long> values, double maxStandardError)
    {
        SqlVarbinary serialized = (SqlVarbinary) AggregationTestUtils.aggregation(
                FUNCTION_RESOLUTION.getAggregateFunction("approx_set", fromTypes(BIGINT, DOUBLE)),
                createPage(values, maxStandardError));
        if (serialized == null) {
            return null;
        }
        return HyperLogLog.newInstance(Slices.wrappedBuffer(serialized.getBytes()));
    }

    private static Page createPage(List<Long> values, double maxStandardError)
    {
        if (values.isEmpty()) {
            return new Page(0);
        }
        BlockBuilder valueBuilder = BIGINT.createBlockBuilder(null, values.size());
        BlockBuilder errorBuilder = DOUBLE.createBlockBuilder(null, values.size());
        for (long value : values) {
            BIGINT.writeLong(valueBuilder, value);
            DOUBLE.writeDouble(errorBuilder, maxStandardError);
        }
        return new Page(values.size(), valueBuilder.build(), errorBuilder.build());
    }

    private static List<Long> sequence(int count)
    {
        List<Long> values = new ArrayList<>(count);
        for (long i = 0; i < count; i++) {
            values.add(i);
        }
        return values;
    }
}
