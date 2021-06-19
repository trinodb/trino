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

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import io.trino.Session;
import io.trino.execution.warnings.WarningCollector;
import io.trino.plugin.tpch.ColumnNaming;
import io.trino.plugin.tpch.TpchConnectorFactory;
import io.trino.testing.LocalQueryRunner;
import io.trino.tpch.Customer;
import org.intellij.lang.annotations.Language;
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
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.plugin.tpch.TpchConnectorFactory.TPCH_COLUMN_NAMING_PROPERTY;
import static io.trino.sql.planner.LogicalPlanner.Stage.OPTIMIZED;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@SuppressWarnings("MethodMayBeStatic")
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Fork(1)
@Measurement(iterations = 20)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPlanner
{
    @SuppressWarnings("FieldMayBeFinal")
    @State(Scope.Benchmark)
    public static class BenchmarkData
    {
        @Param({"optimized", "created"})
        private String stage = OPTIMIZED.toString();

        private LocalQueryRunner queryRunner;
        private List<String> queries;
        @Language("SQL")
        private String largeInQuery;
        private Session session;

        @Setup
        public void setup()
        {
            String tpch = "tpch";

            session = testSessionBuilder()
                    .setCatalog(tpch)
                    .setSchema("sf1")
                    .build();

            queryRunner = LocalQueryRunner.create(session);
            queryRunner.createCatalog(tpch, new TpchConnectorFactory(4), ImmutableMap.of(TPCH_COLUMN_NAMING_PROPERTY, ColumnNaming.STANDARD.name()));

            queries = IntStream.rangeClosed(1, 22)
                    .boxed()
                    .filter(i -> i != 15) // q15 has two queries in it
                    .map(i -> readResource(format("/io/trino/tpch/queries/q%d.sql", i)))
                    .collect(toImmutableList());

            largeInQuery = "SELECT * from orders where o_orderkey in " +
                    IntStream.range(0, 5000)
                    .mapToObj(Integer::toString)
                    .collect(joining(", ", "(", ")"));
        }

        @TearDown
        public void tearDown()
        {
            queryRunner.close();
            queryRunner = null;
        }

        public String readResource(String resource)
        {
            try {
                URL resourceUrl = Customer.class.getResource(resource);
                return Resources.toString(resourceUrl, StandardCharsets.UTF_8);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Benchmark
    public List<Plan> planQueries(BenchmarkData benchmarkData)
    {
        return benchmarkData.queryRunner.inTransaction(transactionSession -> {
            LogicalPlanner.Stage stage = LogicalPlanner.Stage.valueOf(benchmarkData.stage.toUpperCase(ENGLISH));
            return benchmarkData.queries.stream()
                    .map(query -> benchmarkData.queryRunner.createPlan(transactionSession, query, stage, false, WarningCollector.NOOP))
                    .collect(toImmutableList());
        });
    }

    @Benchmark
    public Plan planLargeInQuery(BenchmarkData benchmarkData)
    {
        return benchmarkData.queryRunner.inTransaction(transactionSession -> {
            LogicalPlanner.Stage stage = LogicalPlanner.Stage.valueOf(benchmarkData.stage.toUpperCase(ENGLISH));
            return benchmarkData.queryRunner.createPlan(
                    transactionSession, benchmarkData.largeInQuery, stage, false, WarningCollector.NOOP);
        });
    }

    @Test
    public void verify()
    {
        BenchmarkData data = new BenchmarkData();
        data.setup();
        BenchmarkPlanner benchmark = new BenchmarkPlanner();
        assertEquals(benchmark.planQueries(data).size(), 21);
        assertNotNull(benchmark.planLargeInQuery(data));
    }

    public static void main(String[] args)
            throws Exception
    {
        // assure the benchmarks are valid before running
        BenchmarkData data = new BenchmarkData();
        data.setup();
        try {
            new BenchmarkPlanner().planQueries(data);
        }
        finally {
            data.tearDown();
        }

        benchmark(BenchmarkPlanner.class, WarmupMode.BULK).run();
    }
}
