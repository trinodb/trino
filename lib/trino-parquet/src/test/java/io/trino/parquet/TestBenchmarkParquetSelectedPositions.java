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
package io.trino.parquet;

import org.apache.parquet.format.CompressionCodec;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestBenchmarkParquetSelectedPositions
{
    @Test
    public void testBenchmark()
            throws Exception
    {
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.BOOLEAN, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.INTEGER, BenchmarkParquetSelectedPositions.SelectionShape.PAGE_ALIGNED, 1, 0, 0, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.BIGINT, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 3);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.DOUBLE, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.VARCHAR, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.DATE, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.REAL, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.DECIMAL_9, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.DECIMAL_18, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.DECIMAL_30, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.VARCHAR, BenchmarkParquetSelectedPositions.SelectionShape.RUNS, 8, 32, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.VARCHAR, BenchmarkParquetSelectedPositions.SelectionShape.CONCENTRATED, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.VARCHAR, BenchmarkParquetSelectedPositions.SelectionShape.DISTRIBUTED, 8, 0, 10, 1);
        assertBenchmark(BenchmarkParquetSelectedPositions.DataType.VARCHAR, BenchmarkParquetSelectedPositions.SelectionShape.PAGE_SHIFTED, 8, 0, 10, 1);
        assertBenchmark(
                BenchmarkParquetSelectedPositions.DataType.VARCHAR,
                BenchmarkParquetSelectedPositions.SelectionShape.RUNS,
                1,
                0,
                50,
                3,
                BenchmarkParquetSelectedPositions.ColumnLayout.FIRST_TYPE_THEN_BIGINT);
    }

    @Test
    public void testPageSelectionShapesPreserveSelectivity()
    {
        assertThat(BenchmarkParquetSelectedPositions.SelectionShape.PAGE_ALIGNED.select(0, 1024, 0.75, 1, 128)).hasSize(768);
        assertThat(BenchmarkParquetSelectedPositions.SelectionShape.PAGE_SHIFTED.select(0, 1024, 0.75, 1, 128)).hasSize(768);
        assertThat(BenchmarkParquetSelectedPositions.SelectionShape.SKEWED_RUNS.select(0, 1024, 0.10, 20, 128))
                .hasSize(102)
                .isSorted();
    }

    @Test
    public void testAdaptivePushdownPolicy()
            throws Exception
    {
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.BOOLEAN, 0.05, 0, 0, false);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.05, 0, 0, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.10, 0, 0, false);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.05, 20, 0, 0, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.05, 24, 0, 0, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.05, 28, 0, 0, false);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0.20, 0, 50, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.VARCHAR, 0.25, 0, 0, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.VARCHAR, 0.50, 0, 0, false);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.VARCHAR, 0.10, 16, 0, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.DECIMAL_30, 0.20, 0, 50, true);
        assertAdaptiveDecision(BenchmarkParquetSelectedPositions.DataType.DECIMAL_30, 0.25, 0, 50, false);
    }

    @Test
    public void testAdaptivePagePushdownPolicy()
            throws Exception
    {
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.INTEGER, 0, false);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.BIGINT, 0, false);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.DATE, 0, false);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.REAL, 0, false);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.DOUBLE, 0, true);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.DECIMAL_18, 0, true);
        assertAdaptivePageDecision(BenchmarkParquetSelectedPositions.DataType.DOUBLE, 1_024, false);
    }

    @Test
    public void testLoadedColumnsDoNotDisableWideBinaryPushdown()
            throws Exception
    {
        BenchmarkParquetSelectedPositions benchmark = new BenchmarkParquetSelectedPositions();
        benchmark.compression = CompressionCodec.ZSTD;
        benchmark.dataType = BenchmarkParquetSelectedPositions.DataType.VARCHAR;
        benchmark.valueShape = BenchmarkParquetSelectedPositions.ValueShape.RANDOM;
        benchmark.pushdownDecision = BenchmarkParquetSelectedPositions.PushdownDecision.ADAPTIVE;
        benchmark.columnCount = 5;
        benchmark.preloadedColumnCount = 4;
        benchmark.columnLayout = BenchmarkParquetSelectedPositions.ColumnLayout.FIRST_TYPE_THEN_BIGINT;
        benchmark.selectionShape = BenchmarkParquetSelectedPositions.SelectionShape.RUNS;
        benchmark.selectivity = 0.25;
        benchmark.runCount = 16;
        benchmark.rowCount = 8_192;
        benchmark.pageValueCount = 8_192;
        benchmark.payloadWidth = 1_024;
        benchmark.entropyBytes = 16;
        benchmark.dictionaryCardinality = 0;
        benchmark.dictionaryRunLength = 1;
        benchmark.nullPercentage = 0;
        benchmark.nullShape = BenchmarkParquetSelectedPositions.NullShape.RANDOM;
        benchmark.nullRunCount = 16;
        benchmark.setup();

        BenchmarkParquetSelectedPositions.DataCounters counters = new BenchmarkParquetSelectedPositions.DataCounters();
        benchmark.selectedPositionsPushdown(counters);
        assertThat(counters.selectedPositionsPushdowns).isPositive();
    }

    @Test
    public void testSkewedRunsFallBack()
            throws Exception
    {
        assertAdaptiveDecision(
                BenchmarkParquetSelectedPositions.DataType.INTEGER,
                BenchmarkParquetSelectedPositions.SelectionShape.RUNS,
                true);
        assertAdaptiveDecision(
                BenchmarkParquetSelectedPositions.DataType.INTEGER,
                BenchmarkParquetSelectedPositions.SelectionShape.SKEWED_RUNS,
                false);
    }

    private static void assertAdaptivePageDecision(
            BenchmarkParquetSelectedPositions.DataType dataType,
            int dictionaryCardinality,
            boolean expectedPushdown)
            throws Exception
    {
        BenchmarkParquetSelectedPositions benchmark = new BenchmarkParquetSelectedPositions();
        benchmark.compression = CompressionCodec.ZSTD;
        benchmark.dataType = dataType;
        benchmark.valueShape = BenchmarkParquetSelectedPositions.ValueShape.RANDOM;
        benchmark.pushdownDecision = BenchmarkParquetSelectedPositions.PushdownDecision.ADAPTIVE;
        benchmark.columnCount = 1;
        benchmark.preloadedColumnCount = 0;
        benchmark.columnLayout = BenchmarkParquetSelectedPositions.ColumnLayout.HOMOGENEOUS;
        benchmark.selectionShape = BenchmarkParquetSelectedPositions.SelectionShape.PAGE_ALIGNED;
        benchmark.selectivity = 0.25;
        benchmark.runCount = 1;
        benchmark.rowCount = 65_536;
        benchmark.pageValueCount = 4_096;
        benchmark.payloadWidth = 64;
        benchmark.entropyBytes = 16;
        benchmark.dictionaryCardinality = dictionaryCardinality;
        benchmark.dictionaryRunLength = 1;
        benchmark.nullPercentage = 0;
        benchmark.nullShape = BenchmarkParquetSelectedPositions.NullShape.RANDOM;
        benchmark.nullRunCount = 16;
        benchmark.setup();

        BenchmarkParquetSelectedPositions.DataCounters baselineCounters = new BenchmarkParquetSelectedPositions.DataCounters();
        BenchmarkParquetSelectedPositions.DataCounters pushdownCounters = new BenchmarkParquetSelectedPositions.DataCounters();
        assertThat(benchmark.selectedPositionsPushdown(pushdownCounters))
                .isEqualTo(benchmark.fullDecodeThenSelect(baselineCounters));
        if (expectedPushdown) {
            assertThat(pushdownCounters.selectedPositionsPushdowns)
                    .as("%s dictionary %s", dataType, dictionaryCardinality)
                    .isPositive();
            assertThat(pushdownCounters.selectedPushdownDataPagesRead)
                    .as("%s dictionary %s", dataType, dictionaryCardinality)
                    .isLessThan(baselineCounters.fullDecodeDataPagesRead);
        }
        else {
            assertThat(pushdownCounters.selectedPositionsPushdowns)
                    .as("%s dictionary %s", dataType, dictionaryCardinality)
                    .isZero();
            assertThat(pushdownCounters.selectedPushdownDataPagesRead)
                    .as("%s dictionary %s", dataType, dictionaryCardinality)
                    .isEqualTo(baselineCounters.fullDecodeDataPagesRead);
        }
    }

    private static void assertAdaptiveDecision(
            BenchmarkParquetSelectedPositions.DataType dataType,
            double selectivity,
            int dictionaryCardinality,
            int nullPercentage,
            boolean expectedPushdown)
            throws Exception
    {
        assertAdaptiveDecision(dataType, selectivity, 16, dictionaryCardinality, nullPercentage, expectedPushdown);
    }

    private static void assertAdaptiveDecision(
            BenchmarkParquetSelectedPositions.DataType dataType,
            double selectivity,
            int runCount,
            int dictionaryCardinality,
            int nullPercentage,
            boolean expectedPushdown)
            throws Exception
    {
        BenchmarkParquetSelectedPositions benchmark = new BenchmarkParquetSelectedPositions();
        benchmark.compression = CompressionCodec.ZSTD;
        benchmark.dataType = dataType;
        benchmark.valueShape = BenchmarkParquetSelectedPositions.ValueShape.RANDOM;
        benchmark.pushdownDecision = BenchmarkParquetSelectedPositions.PushdownDecision.ADAPTIVE;
        benchmark.columnCount = 1;
        benchmark.preloadedColumnCount = 0;
        benchmark.columnLayout = BenchmarkParquetSelectedPositions.ColumnLayout.HOMOGENEOUS;
        benchmark.selectionShape = BenchmarkParquetSelectedPositions.SelectionShape.RUNS;
        benchmark.selectivity = selectivity;
        benchmark.runCount = runCount;
        benchmark.rowCount = 8_192;
        benchmark.pageValueCount = 8_192;
        benchmark.payloadWidth = 64;
        benchmark.entropyBytes = 16;
        benchmark.dictionaryCardinality = dictionaryCardinality;
        benchmark.dictionaryRunLength = 1;
        benchmark.nullPercentage = nullPercentage;
        benchmark.nullShape = BenchmarkParquetSelectedPositions.NullShape.RANDOM;
        benchmark.nullRunCount = 16;
        benchmark.setup();

        BenchmarkParquetSelectedPositions.DataCounters counters = new BenchmarkParquetSelectedPositions.DataCounters();
        benchmark.selectedPositionsPushdown(counters);
        if (expectedPushdown) {
            assertThat(counters.selectedPositionsPushdowns)
                    .as("%s selectivity %s, dictionary %s, nulls %s", dataType, selectivity, dictionaryCardinality, nullPercentage)
                    .isPositive();
        }
        else {
            assertThat(counters.selectedPositionsPushdowns)
                    .as("%s selectivity %s, dictionary %s, nulls %s", dataType, selectivity, dictionaryCardinality, nullPercentage)
                    .isZero();
        }
    }

    private static void assertAdaptiveDecision(
            BenchmarkParquetSelectedPositions.DataType dataType,
            BenchmarkParquetSelectedPositions.SelectionShape selectionShape,
            boolean expectedPushdown)
            throws Exception
    {
        BenchmarkParquetSelectedPositions benchmark = new BenchmarkParquetSelectedPositions();
        benchmark.compression = CompressionCodec.ZSTD;
        benchmark.dataType = dataType;
        benchmark.valueShape = BenchmarkParquetSelectedPositions.ValueShape.RANDOM;
        benchmark.pushdownDecision = BenchmarkParquetSelectedPositions.PushdownDecision.ADAPTIVE;
        benchmark.columnCount = 1;
        benchmark.preloadedColumnCount = 0;
        benchmark.columnLayout = BenchmarkParquetSelectedPositions.ColumnLayout.HOMOGENEOUS;
        benchmark.selectionShape = selectionShape;
        benchmark.selectivity = 0.05;
        benchmark.runCount = 20;
        benchmark.rowCount = 65_536;
        benchmark.pageValueCount = 65_536;
        benchmark.payloadWidth = 64;
        benchmark.entropyBytes = 16;
        benchmark.dictionaryCardinality = 0;
        benchmark.dictionaryRunLength = 1;
        benchmark.nullPercentage = 0;
        benchmark.nullShape = BenchmarkParquetSelectedPositions.NullShape.RANDOM;
        benchmark.nullRunCount = 16;
        benchmark.setup();

        BenchmarkParquetSelectedPositions.DataCounters counters = new BenchmarkParquetSelectedPositions.DataCounters();
        benchmark.selectedPositionsPushdown(counters);
        if (expectedPushdown) {
            assertThat(counters.selectedPositionsPushdowns).isPositive();
        }
        else {
            assertThat(counters.selectedPositionsPushdowns).isZero();
        }
    }

    private static void assertBenchmark(
            BenchmarkParquetSelectedPositions.DataType dataType,
            BenchmarkParquetSelectedPositions.SelectionShape selectionShape,
            int runCount,
            int dictionaryCardinality,
            int nullPercentage,
            int columnCount)
            throws Exception
    {
        assertBenchmark(dataType, selectionShape, runCount, dictionaryCardinality, nullPercentage, columnCount, BenchmarkParquetSelectedPositions.ColumnLayout.HOMOGENEOUS);
    }

    private static void assertBenchmark(
            BenchmarkParquetSelectedPositions.DataType dataType,
            BenchmarkParquetSelectedPositions.SelectionShape selectionShape,
            int runCount,
            int dictionaryCardinality,
            int nullPercentage,
            int columnCount,
            BenchmarkParquetSelectedPositions.ColumnLayout columnLayout)
            throws Exception
    {
        BenchmarkParquetSelectedPositions benchmark = new BenchmarkParquetSelectedPositions();
        benchmark.compression = CompressionCodec.ZSTD;
        benchmark.dataType = dataType;
        benchmark.valueShape = BenchmarkParquetSelectedPositions.ValueShape.RANDOM;
        benchmark.pushdownDecision = BenchmarkParquetSelectedPositions.PushdownDecision.ADAPTIVE;
        benchmark.columnCount = columnCount;
        benchmark.preloadedColumnCount = 0;
        benchmark.columnLayout = columnLayout;
        benchmark.selectionShape = selectionShape;
        benchmark.selectivity = 0.1;
        benchmark.runCount = runCount;
        benchmark.rowCount = 1024;
        benchmark.pageValueCount = 128;
        benchmark.payloadWidth = 64;
        benchmark.entropyBytes = 16;
        benchmark.dictionaryCardinality = dictionaryCardinality;
        benchmark.dictionaryRunLength = 1;
        benchmark.nullPercentage = nullPercentage;
        benchmark.nullShape = BenchmarkParquetSelectedPositions.NullShape.RANDOM;
        benchmark.nullRunCount = 16;
        benchmark.setup();

        BenchmarkParquetSelectedPositions.DataCounters baselineCounters = new BenchmarkParquetSelectedPositions.DataCounters();
        BenchmarkParquetSelectedPositions.DataCounters pushdownCounters = new BenchmarkParquetSelectedPositions.DataCounters();
        assertThat(benchmark.fullDecodeThenSelect(baselineCounters))
                .isEqualTo(benchmark.selectedPositionsPushdown(pushdownCounters));
        assertThat(pushdownCounters.selectedRows).isEqualTo(baselineCounters.selectedRows);
        assertThat(pushdownCounters.selectedRuns).isEqualTo(baselineCounters.selectedRuns);
        assertThat(pushdownCounters.selectedDataPages).isEqualTo(baselineCounters.selectedDataPages);
        assertThat(pushdownCounters.dictionaryEncodedColumnChunks + pushdownCounters.nonDictionaryEncodedColumnChunks).isPositive();
        assertThat(pushdownCounters.parquetFileBytes).isPositive();
        assertThat(pushdownCounters.inputBytes).isPositive();
        assertThat(pushdownCounters.parquetUncompressedBytes).isPositive();
        assertThat(pushdownCounters.totalDataPages).isGreaterThanOrEqualTo(pushdownCounters.selectedDataPages);
        assertThat(baselineCounters.fullDecodeDataPagesRead).isPositive();
        assertThat(pushdownCounters.selectedPushdownDataPagesRead).isPositive();
        if (columnLayout != BenchmarkParquetSelectedPositions.ColumnLayout.HOMOGENEOUS) {
            assertThat(pushdownCounters.selectedPositionsPushdowns).isPositive();
        }
    }
}
