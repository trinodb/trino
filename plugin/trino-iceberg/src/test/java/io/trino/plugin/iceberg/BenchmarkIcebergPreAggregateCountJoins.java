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

import io.trino.Session;
import io.trino.operator.aggregation.CountJoinAggregation;
import io.trino.sql.planner.plan.AggregationNode;
import io.trino.sql.planner.plan.PlanNode;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.MaterializedResult;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.SystemSessionProperties.PRE_AGGREGATE_COUNT_JOINS;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.assertions.Assert.assertEqualsIgnoreOrder;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.openjdk.jmh.annotations.Mode.AverageTime;
import static org.openjdk.jmh.annotations.Scope.Benchmark;

/**
 * End-to-end Iceberg/Parquet benchmark with three in-process nodes and local storage.
 * Includes planning, connector scans, exchanges and result consumption. Table creation,
 * ANALYZE and correctness/plan assertions run outside the measured iterations.
 */
@State(Benchmark)
@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode(AverageTime)
@Fork(2)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkIcebergPreAggregateCountJoins
{
    @Param({"expanding", "moderate", "unique"})
    public String shape;

    @Param({"baseline", "optimized", "manual"})
    public String strategy;

    private DistributedQueryRunner runner;
    private Session session;
    private String query;
    private MaterializedResult expected;

    @Setup
    public void setup()
            throws Exception
    {
        int keys = switch (shape) {
            case "expanding" -> 100;
            case "moderate" -> 1000;
            case "unique" -> 100_000;
            default -> throw new IllegalArgumentException("Unknown shape: " + shape);
        };
        runner = IcebergQueryRunner.builder()
                .setWorkerCount(2)
                .addCoordinatorProperty("node-scheduler.include-coordinator", "false")
                .build();
        try {
            runner.execute("CREATE TABLE join_left WITH (format = 'PARQUET') AS " +
                    "SELECT CAST(mod(a * 1000 + b, " + keys + ") AS bigint) k, CAST(mod(a * 1000 + b, 3) AS bigint) g " +
                    "FROM UNNEST(sequence(0, 99)) t(a) CROSS JOIN UNNEST(sequence(0, 999)) u(b)");
            runner.execute("CREATE TABLE join_right WITH (format = 'PARQUET') AS SELECT * FROM join_left");
            runner.execute("ANALYZE join_left");
            runner.execute("ANALYZE join_right");
            configureStrategy();

            // Compute the expected multiset independently of the engine and optimizer.
            long[][] frequencies = new long[keys][3];
            for (int row = 0; row < 100_000; row++) {
                frequencies[row % keys][row % 3]++;
            }
            MaterializedResult.Builder expected = MaterializedResult.resultBuilder(BIGINT, BIGINT, BIGINT);
            for (int left = 0; left < 3; left++) {
                for (int right = 0; right < 3; right++) {
                    long count = 0;
                    for (long[] frequency : frequencies) {
                        count += frequency[left] * frequency[right];
                    }
                    if (count > 0) {
                        expected.row((long) left, (long) right, count);
                    }
                }
            }
            this.expected = expected.build();
            validate();
            System.out.println("BENCHMARK_STATS " + runner.execute("SHOW STATS FOR join_left"));
        }
        catch (Exception | Error e) {
            runner.close();
            throw e;
        }
    }

    private void configureStrategy()
    {
        session = Session.builder(runner.getDefaultSession())
                .setSystemProperty(PRE_AGGREGATE_COUNT_JOINS, Boolean.toString(strategy.equals("optimized")))
                .build();
        query = switch (strategy) {
            case "baseline", "optimized" -> "SELECT l.g, r.g, count(*) FROM join_left l JOIN join_right r ON l.k = r.k GROUP BY l.g, r.g";
            // Safe for these bounded, nonempty fixtures; not a general replacement for COUNT semantics.
            case "manual" -> "SELECT l.g, r.g, sum(l.n * r.n) " +
                    "FROM (SELECT k, g, count(*) n FROM join_left GROUP BY k, g) l " +
                    "JOIN (SELECT k, g, count(*) n FROM join_right GROUP BY k, g) r ON l.k = r.k GROUP BY l.g, r.g";
            default -> throw new IllegalArgumentException("Unknown strategy: " + strategy);
        };
    }

    private void validate()
    {
        var result = runner.executeWithPlan(session, query);
        assertEqualsIgnoreOrder(result.result().getMaterializedRows(), expected.getMaterializedRows());
        checkState(hasCountJoin(result.queryPlan().orElseThrow().getRoot()) == (strategy.equals("optimized") && !shape.equals("unique")), "Unexpected benchmark plan");
        checkState(runner.getCoordinator().getFullQueryInfo(result.queryId()).getQueryStats().getPhysicalInputDataSize().toBytes() > 0, "Benchmark must read Iceberg files");
        System.out.println("BENCHMARK_PLAN shape=" + shape + " strategy=" + strategy);
        System.out.println(runner.execute(session, "EXPLAIN " + query).getOnlyValue());
    }

    @TearDown
    public void close()
    {
        runner.close();
    }

    @Benchmark
    public MaterializedResult execute()
    {
        return runner.execute(session, query);
    }

    private static boolean hasCountJoin(PlanNode node)
    {
        if (node instanceof AggregationNode aggregation && aggregation.getAggregations().values().stream()
                .anyMatch(value -> value.getResolvedFunction().signature().getName().functionName().equals(CountJoinAggregation.NAME))) {
            return true;
        }
        return node.getSources().stream().anyMatch(BenchmarkIcebergPreAggregateCountJoins::hasCountJoin);
    }

    private static void runDiagnostics()
            throws Exception
    {
        String[] strategies = {"baseline", "optimized", "manual"};
        for (String shape : new String[] {"expanding", "moderate", "unique"}) {
            BenchmarkIcebergPreAggregateCountJoins benchmark = new BenchmarkIcebergPreAggregateCountJoins();
            benchmark.shape = shape;
            benchmark.strategy = "baseline";
            benchmark.setup();
            try {
                for (String strategy : strategies) {
                    benchmark.strategy = strategy;
                    benchmark.configureStrategy();
                    benchmark.validate();
                }
                // Rotate order across three warmup and seven measured triplets on the same tables.
                for (int round = 0; round < 10; round++) {
                    for (int offset = 0; offset < strategies.length; offset++) {
                        benchmark.strategy = strategies[(round + offset) % strategies.length];
                        benchmark.configureStrategy();
                        long start = System.nanoTime();
                        var result = benchmark.runner.executeWithPlan(benchmark.session, benchmark.query);
                        double wallMillis = (System.nanoTime() - start) / 1_000_000.0;
                        assertEqualsIgnoreOrder(result.result().getMaterializedRows(), benchmark.expected.getMaterializedRows());
                        var stats = benchmark.runner.getCoordinator().getFullQueryInfo(result.queryId()).getQueryStats();
                        checkState(stats.getPhysicalInputDataSize().toBytes() > 0, "Missing Iceberg reads");
                        if (round >= 3) {
                            System.out.println("ICEBERG_SAMPLE,%s,%s,%s,%s,%s,%s,%s,%s,%s".formatted(
                                    shape,
                                    benchmark.strategy,
                                    round - 3,
                                    wallMillis,
                                    stats.getTotalCpuTime().getValue(NANOSECONDS) / 1_000_000.0,
                                    stats.getPhysicalInputDataSize().toBytes(),
                                    stats.getInternalNetworkInputDataSize().toBytes(),
                                    stats.getPeakUserMemoryReservation().toBytes(),
                                    stats.getOperatorSummaries().stream()
                                            .filter(operator -> operator.getOperatorType().endsWith("JoinOperator"))
                                            .mapToLong(operator -> operator.getOutputPositions()).sum()));
                        }
                    }
                }
            }
            finally {
                benchmark.close();
            }
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        if (args.length == 1 && args[0].equals("diagnostics")) {
            runDiagnostics();
            return;
        }
        benchmark(BenchmarkIcebergPreAggregateCountJoins.class)
                .withOptions(options -> options
                        .shouldFailOnError(true)
                        .jvmArgsAppend("--add-modules=jdk.incubator.vector", "--enable-native-access=ALL-UNNAMED", "-Xmx4g", "-XX:ActiveProcessorCount=4"))
                .run();
    }
}
