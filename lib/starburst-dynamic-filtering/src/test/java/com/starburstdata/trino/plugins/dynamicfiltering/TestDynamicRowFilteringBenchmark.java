/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package com.starburstdata.trino.plugins.dynamicfiltering;

import org.testng.annotations.Test;

import static com.starburstdata.trino.plugins.dynamicfiltering.BenchmarkDynamicPageFilter.DataSet;

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
        BenchmarkDynamicPageFilter benchmark = new BenchmarkDynamicPageFilter(dataSet);
        benchmark.setup();
        benchmark.filterPages();
    }
}
