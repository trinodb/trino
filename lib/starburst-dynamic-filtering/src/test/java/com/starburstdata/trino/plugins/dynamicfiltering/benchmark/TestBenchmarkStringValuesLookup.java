/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering.benchmark;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;

import static com.starburstdata.trino.plugins.dynamicfiltering.benchmark.BenchmarkStringValuesLookup.InputData;
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
                BenchmarkStringValuesLookup benchMark = new BenchmarkStringValuesLookup(input, selectivity);
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
                BenchmarkStringValuesLookup benchMark = new BenchmarkStringValuesLookup(input, selectivity);
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
