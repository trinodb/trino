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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.connector.MockConnectorFactory;
import io.trino.connector.MockConnectorPlugin;
import io.trino.connector.MockConnectorTableHandle;
import io.trino.execution.querystats.PlanOptimizersStatsCollector;
import io.trino.plugin.tpch.ColumnNaming;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.SchemaTableName;
import io.trino.sql.planner.LogicalPlanner.Stage;
import io.trino.sql.planner.optimizations.PlanOptimizer;
import io.trino.testing.PlanTester;
import io.trino.tpch.Customer;
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
import org.openjdk.jmh.runner.options.WarmupMode;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.execution.querystats.PlanOptimizersStatsCollector.createPlanOptimizersStatsCollector;
import static io.trino.execution.warnings.WarningCollector.NOOP;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.planner.BenchmarkPlanner.Queries.TPCH;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Fork(1)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPlanner
{
    private static final SchemaTableName TABLE = new SchemaTableName("default", "t");

    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"OPTIMIZED", "CREATED"})
        private Stage stage = OPTIMIZED;
        @Param
        private Queries queries = TPCH;

        private PlanTester planTester;
        private Session session;

        @Setup
        public void setup()
        {
            String tpch = "tpch";

            session = testSessionBuilder()
                    .setCatalog(tpch)
                    .setSchema("sf1")
                    .build();

            planTester = PlanTester.create(session);
            planTester.installPlugin(new TpchPlugin());
            planTester.createCatalog(tpch, "tpch", ImmutableMap.<String, String>builder()
                    .put("tpch.splits-per-node", "4")
                    .put("tpch.column-naming", ColumnNaming.STANDARD.name())
                    .buildOrThrow());

            planTester.installPlugin(new MockConnectorPlugin(MockConnectorFactory.builder()
                    .withGetTableHandle((session1, schemaTableName) -> new MockConnectorTableHandle(schemaTableName))
                    .withGetColumns(name -> {
                        if (!name.equals(TABLE)) {
                            throw new IllegalArgumentException();
                        }
                        return IntStream.rangeClosed(0, 500)
                                .mapToObj(i -> new ColumnMetadata("col_varchar_" + i, VARCHAR))
                                .collect(toImmutableList());
                    })
                    .build()));
            planTester.createCatalog("mock", "mock", ImmutableMap.of());
        }

        @TearDown
        public void tearDown()
        {
            planTester.close();
            planTester = null;
        }
    }

    @Benchmark
    public List<Plan> plan(BenchmarkData benchmarkData)
    {
        PlanTester planTester = benchmarkData.planTester;
        List<PlanOptimizer> planOptimizers = planTester.getPlanOptimizers(false);
        PlanOptimizersStatsCollector planOptimizersStatsCollector = createPlanOptimizersStatsCollector();
        return planTester.inTransaction(transactionSession -> benchmarkData.queries.getQueries().stream()
                .map(query -> planTester.createPlan(transactionSession, query, planOptimizers, benchmarkData.stage, NOOP, planOptimizersStatsCollector))
                .collect(toImmutableList()));
    }

    @Test
    public void verify()
    {
        BenchmarkPlanner benchmark = new BenchmarkPlanner();
        for (Queries queries : Queries.values()) {
            BenchmarkData data = new BenchmarkData();
            data.queries = queries;
            data.setup();
            assertThat(benchmark.plan(data)).isNotNull();
        }
    }

    public static enum Queries
    {
        TPCH(() -> IntStream.rangeClosed(1, 22)
                .boxed()
                .filter(i -> i != 15) // q15 has two queries in it
                .map(i -> readResource(format("/io/trino/tpch/queries/q%d.sql", i)))
                .collect(toImmutableList())),
        LARGE_IN(() -> ImmutableList.of("SELECT * from orders where o_orderkey in " +
                IntStream.range(0, 5000)
                        .mapToObj(Integer::toString)
                        .collect(joining(", ", "(", ")")))),
        // 86k columns present in the query with 500 group bys
        MULTIPLE_GROUP_BY(() -> ImmutableList.of("WITH " + IntStream.rangeClosed(0, 500)
                .mapToObj(
                        """
                        t%s AS (
                        SELECT * FROM lineitem a
                        JOIN tiny.lineitem b ON a.l_orderkey = b.l_orderkey
                        JOIN sf10.lineitem c ON a.l_orderkey = c.l_orderkey
                        JOIN sf100.lineitem d ON a.l_orderkey = d.l_orderkey
                        JOIN sf1000.lineitem e ON a.l_orderkey = e.l_orderkey
                        WHERE a.l_orderkey = (SELECT max(o_orderkey) FROM orders GROUP BY o_orderkey))
                        """::formatted)
                .collect(joining(",")) +
                "SELECT 1 FROM lineitem")),
        GROUP_BY_WITH_MANY_REFERENCED_COLUMNS(() -> ImmutableList.of("SELECT * FROM mock.default.t GROUP BY " +
                IntStream.rangeClosed(1, 501)
                        .mapToObj(Integer::toString)
                        .collect(joining(",")))),
        /**/;

        private final Supplier<List<String>> queries;

        Queries(Supplier<List<String>> queries)
        {
            this.queries = requireNonNull(queries, "queries is null");
        }

        public List<String> getQueries()
        {
            return queries.get();
        }
    }

    private static String readResource(String resource)
    {
        try {
            URL resourceUrl = Customer.class.getResource(resource);
            return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        try {
            new BenchmarkPlanner().plan(data);
        }
        finally {
            data.tearDown();
        }

        benchmark(BenchmarkPlanner.class, WarmupMode.BULK).run();
    }
}
