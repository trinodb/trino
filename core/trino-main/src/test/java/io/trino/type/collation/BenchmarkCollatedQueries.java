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
package io.trino.type.collation;

import com.google.common.collect.ImmutableMap;
import io.trino.Session;
import io.trino.plugin.tpch.TpchPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;
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

import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.testing.TestingSession.testSessionBuilder;

/// Grouping and joining collated strings, against the same query under the default collation.
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(value = 1, jvmArgsAppend = {"-Xmx6g", "--add-modules=jdk.incubator.vector", "--sun-misc-unsafe-memory-access=allow", "--enable-native-access=ALL-UNNAMED", "-Duser.timezone=America/Bahia_Banderas"})
@Warmup(iterations = 5)
@Measurement(iterations = 8)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkCollatedQueries
{
    // the customer name in alternating case, so that names equal under unicode_ci are distinct strings
    private static final String KEYS = "SELECT CAST(CASE WHEN custkey %% 2 = 0 THEN upper(name) ELSE lower(name) END AS varchar COLLATE %s) k FROM customer";

    @Param({"unicode_ci", "ucs_basic"})
    private String collation;

    @Param({"group", "join"})
    private String query;

    private QueryRunner queryRunner;
    private String sql;

    @Setup
    public void setup()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("sf1")
                .build();
        queryRunner = new StandaloneQueryRunner(session);
        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch", ImmutableMap.of());
        String keys = KEYS.formatted(collation);
        sql = switch (query) {
            case "group" -> "SELECT count(*) FROM (SELECT k, count(*) FROM (%s) GROUP BY k)".formatted(keys);
            case "join" -> "SELECT count(*) FROM (%s) a JOIN (%s) b ON a.k = b.k".formatted(keys, keys);
            default -> throw new IllegalArgumentException(query);
        };
    }

    @TearDown
    public void tearDown()
    {
        queryRunner.close();
    }

    @Benchmark
    public MaterializedResult run()
    {
        return queryRunner.execute(sql);
    }

    static void main()
            throws Exception
    {
        benchmark(BenchmarkCollatedQueries.class).run();
    }
}
