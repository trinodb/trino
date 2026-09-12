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
package io.trino.typesolver.verifier;

import io.trino.lib.TrinoPreset;
import io.trino.lib.TypeBridge;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.analyzer.TypeDescriptorProvider;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.weakref.solver.Expression;
import org.weakref.solver.FunctionResolver;
import org.weakref.solver.TypeLibrary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeDescriptor;

/**
 * Per-resolution latency of the constraint solver vs Trino's function resolution, over the same
 * calls. Reported as average ns per batch of {@link #CALLS} resolutions; divide by {@code CALLS.size()}
 * for per-call latency.
 * <p>
 * What the integration cares about is the <em>cold</em> (cache-miss) cost, because the steady state
 * is identical: Trino caches resolved functions ({@code BuiltinFunctionResolver}), and a
 * solver-backed resolver would sit behind the same cache. The four arms make that explicit:
 * <ul>
 *   <li>{@link #solverCold} — the solver doing full work every call (no cache). The real cost of a
 *       cache miss, and the number that matters for cold-path latency.</li>
 *   <li>{@link #trinoUncached} — Trino's resolution doing full work every call, via
 *       {@code resolveBuiltinFunctionUncached} which bypasses the cache. The apples-to-apples
 *       cold-vs-cold comparison: this is the SignatureBinder-based work the solver would replace.</li>
 *   <li>{@link #solverCached} — the solver behind a trivial memo, i.e. the steady-state cost once a
 *       call shape has been seen. Shows what caching buys and what integration would actually run.</li>
 *   <li>{@link #trinoCached} — Trino's production path as the analyzer hits it (warm cache), for
 *       reference on the steady-state target.</li>
 * </ul>
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(1)
@Warmup(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
@Measurement(iterations = 5, time = 1, timeUnit = TimeUnit.SECONDS)
public class BenchmarkFunctionResolution
{
    // A spread from trivial (single overload) to hard (coercion + multi-overload + calculated type).
    private static final List<Call> CALLS = List.of(
            new Call("abs", List.of("bigint")),
            new Call("abs", List.of("decimal(10,2)")),
            new Call("concat", List.of("varchar(5)", "varchar(10)")),
            new Call("array_distinct", List.of("array(bigint)")),
            new Call("element_at", List.of("array(varchar(5))", "bigint")),
            new Call("greatest", List.of("integer", "bigint")),
            new Call("round", List.of("decimal(18,4)")),
            new Call("mod", List.of("decimal(10,2)", "decimal(8,4)")),
            new Call("mod", List.of("decimal(10,2)", "tinyint")),
            new Call("regexp_replace", List.of("varchar(10)", "varchar(2)", "varchar(3)")),
            new Call("from_unixtime", List.of("double")),
            new Call("split_part", List.of("varchar(10)", "varchar(2)", "bigint")));

    private TypeLibrary solver;
    private TestingFunctionResolution cachedResolution;
    private List<String> names;
    private List<List<Expression>> solverArguments;
    private List<List<TypeDescriptorProvider>> trinoArguments;
    private Map<List<Object>, FunctionResolver.ResolutionOutcome> solverCache;

    @Setup
    public void setup()
    {
        cachedResolution = new TestingFunctionResolution();
        PlannerContext plannerContext = cachedResolution.getPlannerContext();
        TypeManager typeManager = plannerContext.getTypeManager();

        solver = TrinoPreset.library();
        solverCache = new HashMap<>();
        names = new ArrayList<>();
        solverArguments = new ArrayList<>();
        trinoArguments = new ArrayList<>();
        for (Call call : CALLS) {
            List<Type> argumentTypes = call.arguments().stream()
                    .map(signature -> typeManager.getType(parseTypeDescriptor(signature)))
                    .toList();
            names.add(call.name());
            solverArguments.add(argumentTypes.stream().map(TypeBridge::toExpression).toList());
            trinoArguments.add(TypeDescriptorProvider.fromTypes(argumentTypes));
        }
    }

    @Benchmark
    public void solverCold(Blackhole blackhole)
    {
        for (int i = 0; i < CALLS.size(); i++) {
            blackhole.consume(solver.resolveFunction(names.get(i), solverArguments.get(i)));
        }
    }

    @Benchmark
    public void solverCached(Blackhole blackhole)
    {
        for (int i = 0; i < CALLS.size(); i++) {
            int index = i;
            blackhole.consume(solverCache.computeIfAbsent(
                    List.of(names.get(i), solverArguments.get(i)),
                    _ -> solver.resolveFunction(names.get(index), solverArguments.get(index))));
        }
    }

    @Benchmark
    public void trinoCached(Blackhole blackhole)
    {
        for (int i = 0; i < CALLS.size(); i++) {
            blackhole.consume(cachedResolution.resolveFunction(names.get(i), trinoArguments.get(i)));
        }
    }

    @Benchmark
    public void trinoUncached(Blackhole blackhole)
    {
        for (int i = 0; i < CALLS.size(); i++) {
            blackhole.consume(cachedResolution.resolveBuiltinFunctionUncached(names.get(i), trinoArguments.get(i)));
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        new Runner(new OptionsBuilder()
                .include(BenchmarkFunctionResolution.class.getSimpleName())
                .forks(Integer.getInteger("benchmark.forks", 1))
                .build())
                .run();
    }

    /**
     * In-build entry point: run with {@code mvn test -Dtest=BenchmarkFunctionResolution -DrunBenchmarks=true}.
     * Uses {@code forks(0)} so JMH runs inside the surefire JVM (which already has the incubator Vector
     * module and add-opens that a bare {@code exec:java} JVM lacks); trades fork isolation for being
     * runnable from the build. Standalone rigorous runs go through {@link #main} with the default fork.
     */
    @Test
    @EnabledIfSystemProperty(named = "runBenchmarks", matches = "true")
    void runBenchmark()
            throws Exception
    {
        new Runner(new OptionsBuilder()
                .include(BenchmarkFunctionResolution.class.getSimpleName())
                .forks(0)
                .warmupIterations(3)
                .measurementIterations(5)
                .build())
                .run();
    }

    private record Call(String name, List<String> arguments) {}
}
