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
package io.trino.plugin.deltalake.transactionlog.statistics;

import io.trino.plugin.deltalake.DeltaLakeColumnHandle;
import io.trino.spi.type.BigintType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static io.trino.plugin.deltalake.DeltaLakeColumnType.REGULAR;
import static java.lang.String.format;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 4)
@Fork(1)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkExtendedStatistics
{
    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"JSON", "PARQUET"})
        private String type = "JSON";
        @Param("200")
        private int filesCount = 200;
        @Param({"5", "10", "20", "50", "200"})
        private int columnsCount = 5;
        @Param("100")
        private int queries = 100;

        private Random random = new Random(1);

        private List<DeltaLakeColumnHandle> columns;

        private List<DeltaLakeFileStatistics> fileStatistics;

        @Setup
        public void setup()
        {
            columns = new ArrayList<>(columnsCount);
            for (int i = 0; i < columnsCount; i++) {
                columns.add(new DeltaLakeColumnHandle("column_" + i, BigintType.BIGINT, OptionalInt.empty(), "column_" + i, BigintType.BIGINT, REGULAR));
            }

            fileStatistics = new ArrayList<>(filesCount);
            for (int i = 0; i < filesCount; i++) {
                fileStatistics.add(createSingleFileStatistics());
            }
        }

        private DeltaLakeFileStatistics createSingleFileStatistics()
        {
            switch (type) {
                case "JSON":
                    return new DeltaLakeJsonFileStatistics(
                            Optional.of(random.nextLong()),
                            createColumnValueMap(),
                            createColumnValueMap(),
                            createColumnValueMap());
                case "PARQUET":
                    return new DeltaLakeParquetFileStatistics(
                            Optional.of(random.nextLong()),
                            createColumnValueMap(),
                            createColumnValueMap(),
                            createColumnValueMap());
            }
            throw new IllegalArgumentException("invalid stats type: " + type);
        }

        private Optional<Map<String, Object>> createColumnValueMap()
        {
            Map<String, Object> map = new HashMap<>();
            for (DeltaLakeColumnHandle column : columns) {
                map.put(column.getName(), random.nextLong());
            }
            return Optional.of(map);
        }
    }

    @Benchmark
    public long benchmark(BenchmarkData benchmarkData)
    {
        long result = 1;
        for (DeltaLakeFileStatistics statistics : benchmarkData.fileStatistics) {
            for (int i = 0; i < benchmarkData.queries; i++) {
                DeltaLakeColumnHandle column = benchmarkData.columns.get(benchmarkData.random.nextInt(benchmarkData.columnsCount));
                result += (long) statistics.getMaxColumnValue(column).get();
                result += (long) statistics.getMinColumnValue(column).get();
                result += statistics.getNullCount(column.getName()).get();
            }
        }
        return result;
    }

    @Test
    public void testBenchmark()
    {
        BenchmarkData benchmarkData = new BenchmarkData();
        benchmarkData.setup();
        benchmark(benchmarkData);
    }

    public static void main(String[] args)
            throws Exception
    {
        Options options = new OptionsBuilder()
                .verbosity(VerboseMode.NORMAL)
                .include(".*" + BenchmarkExtendedStatistics.class.getSimpleName() + ".*")
                .resultFormat(ResultFormatType.JSON)
                .result(format("%s/%s-result-%s.json", System.getProperty("java.io.tmpdir"), BenchmarkExtendedStatistics.class.getSimpleName(), ISO_DATE_TIME.format(LocalDateTime.now())))
                .build();
        new Runner(options).run();
    }
}
