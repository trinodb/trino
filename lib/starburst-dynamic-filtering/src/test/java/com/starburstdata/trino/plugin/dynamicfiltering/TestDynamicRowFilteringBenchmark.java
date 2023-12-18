/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugin.dynamicfiltering;

import org.junit.jupiter.api.Test;

import static com.starburstdata.trino.plugin.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet;

public class TestDynamicRowFilteringBenchmark
{
    @Test
    public void testAllDataSets()
    {
        for (DataSet dataSet : DataSet.values()) {
            executeBenchmark(dataSet);
        }
    }

    private static void executeBenchmark(DataSet dataSet)
    {
        BenchmarkDynamicPageFilter benchmark = new BenchmarkDynamicPageFilter();
        benchmark.inputDataSet = dataSet;
        benchmark.setup();
        benchmark.filterPages();
    }
}
