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
package io.trino.plugin.hive.dynamicfiltering;

import com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.hive.dynamicfiltering.BenchmarkStringValuesLookup.InputData;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.withinPercentage;

public class TestBenchmarkStringValuesLookup
{
    @Test
    public void testLookupInSliceBloomFilter()
    {
        for (InputData input : InputData.values()) {
            List<Double> selectivities;
            if (input == InputData.CUSTOMER_MARKET_SEGMENT) {
                selectivities = ImmutableList.of(0.2, 0.4, 0.6, 0.8);
            }
            else {
                selectivities = ImmutableList.of(0.005, 0.01, 0.05, 0.1, 0.2, 0.5, 0.75);
            }
            for (double selectivity : selectivities) {
                BenchmarkStringValuesLookup benchMark = new BenchmarkStringValuesLookup();
                benchMark.inputData = input;
                benchMark.selectivity = selectivity;
                benchMark.setup();
                int inputSize = benchMark.getInputSize();
                double actualSelectivityFromSet = (double) benchMark.lookupInFastHashSet() / inputSize;
                String description = format(
                        "Selectivity for input with actualSelectivityFromSet %f, inputSize %d, hashSetSize %d",
                        actualSelectivityFromSet,
                        inputSize,
                        benchMark.getFilterDistinctValues());
                assertThat(actualSelectivityFromSet)
                        .as(description)
                        .isCloseTo(selectivity, withinPercentage(5));
                assertThat((double) benchMark.lookupInSliceBloomFilter() / inputSize)
                        .as(description)
                        .isBetween(actualSelectivityFromSet, actualSelectivityFromSet + 0.014);
            }
        }
    }

    @Test
    public void testLookupInBloomFilter()
    {
        for (InputData input : InputData.values()) {
            for (double selectivity : ImmutableList.of(0.2, 0.4, 0.6)) {
                BenchmarkStringValuesLookup benchMark = new BenchmarkStringValuesLookup();
                benchMark.inputData = input;
                benchMark.selectivity = selectivity;
                benchMark.setup();
                int inputSize = benchMark.getInputSize();
                double actualSelectivityFromSet = (double) benchMark.lookupInFastHashSet() / inputSize;
                String description = format(
                        "Selectivity for input with actualSelectivityFromSet %f, inputSize %d, hashSetSize %d",
                        actualSelectivityFromSet,
                        inputSize,
                        benchMark.getFilterDistinctValues());
                assertThat(actualSelectivityFromSet)
                        .as(description)
                        .isCloseTo(selectivity, withinPercentage(5));
                assertThat((double) benchMark.lookupInBloomFilter() / inputSize)
                        .isBetween(actualSelectivityFromSet, actualSelectivityFromSet + 0.06);
            }
        }
    }
}
