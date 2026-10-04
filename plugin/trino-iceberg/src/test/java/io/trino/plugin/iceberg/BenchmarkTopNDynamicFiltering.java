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
package io.trino.plugin.iceberg;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.execution.QueryStats;
import io.trino.spi.QueryId;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner.MaterializedResultWithPlan;
import org.junit.jupiter.api.Test;
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
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.SystemSessionProperties.ENABLE_TOP_N_DYNAMIC_FILTERING;
import static io.trino.SystemSessionProperties.MAX_DRIVERS_PER_TASK;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Compares TopN queries with and without TopN dynamic filtering, on a Parquet table sorted by {@code orderkey}.
 * Run {@link #main()} to print the data read by each query and then run the benchmark.
 */
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 10)
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkTopNDynamicFiltering
{
    private static final Map<String, String> QUERIES = ImmutableMap.<String, String>builder()
            // splits of a file are read in order, so the lowest values of each file are read first
            .put("ASCENDING_CLUSTERED_KEY", "SELECT * FROM lineitem ORDER BY orderkey, linenumber LIMIT 10")
            // the highest values of each file are read last, so the bound becomes tight late
            .put("DESCENDING_CLUSTERED_KEY", "SELECT * FROM lineitem ORDER BY orderkey DESC, linenumber DESC LIMIT 10")
            .put("CLUSTERED_KEY_WITH_FILTER", "SELECT * FROM lineitem WHERE shipmode = 'AIR' ORDER BY orderkey, linenumber LIMIT 10")
            .put("CLUSTERED_KEY_LARGE_LIMIT", "SELECT orderkey, linenumber, partkey FROM lineitem ORDER BY orderkey, linenumber LIMIT 10000")
            // no data can be skipped, which shows the overhead of the dynamic filter
            .put("UNCLUSTERED_KEY", "SELECT * FROM lineitem ORDER BY partkey, orderkey, linenumber LIMIT 10")
            .buildOrThrow();

    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"ASCENDING_CLUSTERED_KEY", "DESCENDING_CLUSTERED_KEY", "CLUSTERED_KEY_WITH_FILTER", "CLUSTERED_KEY_LARGE_LIMIT", "UNCLUSTERED_KEY"})
        private String query = "ASCENDING_CLUSTERED_KEY";

        @Param({"true", "false"})
        private boolean topNDynamicFiltering = true;

        @Param("sf1")
        private String tpchSchema = "sf1";

        private DistributedQueryRunner queryRunner;
        private Session session;

        @Setup
        public void setup()
                throws Exception
        {
            queryRunner = createQueryRunner(tpchSchema, 10_000);
            session = createSession(queryRunner, topNDynamicFiltering, "1MB");
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }
    }

    @Benchmark
    public MaterializedResult topN(BenchmarkData data)
    {
        return data.queryRunner.execute(data.session, QUERIES.get(data.query));
    }

    @Test
    public void testDataSkipped()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner("tiny", 1_000)) {
            // run the splits of a task one at a time, so that later splits are read with the bound of earlier ones
            Session enabled = Session.builder(createSession(queryRunner, true, "64kB"))
                    .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                    .build();
            Session disabled = Session.builder(createSession(queryRunner, false, "64kB"))
                    .setSystemProperty(MAX_DRIVERS_PER_TASK, "1")
                    .build();

            for (Map.Entry<String, String> query : QUERIES.entrySet()) {
                MaterializedResultWithPlan withFiltering = queryRunner.executeWithPlan(enabled, query.getValue());
                MaterializedResultWithPlan withoutFiltering = queryRunner.executeWithPlan(disabled, query.getValue());
                assertThat(withFiltering.result().getMaterializedRows())
                        .describedAs(query.getKey())
                        .isEqualTo(withoutFiltering.result().getMaterializedRows());

                if (query.getKey().equals("ASCENDING_CLUSTERED_KEY")) {
                    assertThat(queryStats(queryRunner, withFiltering.queryId()).getPhysicalInputPositions())
                            .isLessThan(queryStats(queryRunner, withoutFiltering.queryId()).getPhysicalInputPositions() / 4);
                }
            }
        }
    }

    private static DistributedQueryRunner createQueryRunner(String tpchSchema, int rowGroupRowCount)
            throws Exception
    {
        DistributedQueryRunner queryRunner = IcebergQueryRunner.builder()
                .setWorkerCount(0)
                .build();
        Session writeSession = Session.builder(queryRunner.getDefaultSession())
                .setCatalogSessionProperty(ICEBERG_CATALOG, "parquet_writer_row_group_max_row_count", Integer.toString(rowGroupRowCount))
                .build();
        queryRunner.execute(
                writeSession,
                "CREATE TABLE lineitem WITH (format = 'PARQUET', sorted_by = ARRAY['orderkey']) AS SELECT * FROM tpch.%s.lineitem".formatted(tpchSchema));
        return queryRunner;
    }

    private static Session createSession(DistributedQueryRunner queryRunner, boolean topNDynamicFiltering, String maxSplitSize)
    {
        return Session.builder(queryRunner.getDefaultSession())
                .setSystemProperty(ENABLE_TOP_N_DYNAMIC_FILTERING, Boolean.toString(topNDynamicFiltering))
                // many splits per file, so that splits opened after the bound is set can skip row groups
                .setCatalogSessionProperty(ICEBERG_CATALOG, "max_split_size", maxSplitSize)
                .build();
    }

    private static QueryStats queryStats(DistributedQueryRunner queryRunner, QueryId queryId)
    {
        return queryRunner.getCoordinator()
                .getQueryManager()
                .getFullQueryInfo(queryId)
                .getQueryStats();
    }

    private static void printDataRead()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = createQueryRunner("sf1", 10_000)) {
            System.out.printf("%-28s %-9s %14s %14s %10s%n", "query", "filtering", "rows read", "bytes read", "wall ms");
            for (Map.Entry<String, String> query : QUERIES.entrySet()) {
                for (boolean topNDynamicFiltering : new boolean[] {true, false}) {
                    MaterializedResultWithPlan result = queryRunner.executeWithPlan(createSession(queryRunner, topNDynamicFiltering, "1MB"), query.getValue());
                    QueryStats stats = queryStats(queryRunner, result.queryId());
                    System.out.printf(
                            "%-28s %-9s %14d %14s %10d%n",
                            query.getKey(),
                            topNDynamicFiltering,
                            stats.getPhysicalInputPositions(),
                            stats.getPhysicalInputDataSize().succinct(),
                            stats.getElapsedTime().toMillis());
                }
            }
        }
    }

    static void main()
            throws Exception
    {
        printDataRead();
        benchmark(BenchmarkTopNDynamicFiltering.class).run();
    }
}
