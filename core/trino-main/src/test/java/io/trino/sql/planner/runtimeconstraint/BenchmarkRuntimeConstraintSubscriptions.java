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
package io.trino.sql.planner.runtimeconstraint;

import com.google.common.collect.ImmutableList;
import io.trino.spi.predicate.Domain;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintNullMatchMode.ORDINARY;
import static io.trino.sql.planner.runtimeconstraint.RuntimeConstraintTransform.IDENTITY;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 3, time = 500, timeUnit = TimeUnit.MILLISECONDS)
public class BenchmarkRuntimeConstraintSubscriptions
{
    private static final RuntimeConstraintId ROOT = new RuntimeConstraintId("root");
    private static final RuntimeConstraintSnapshot VALUE = RuntimeConstraintSnapshot.finalSnapshot(
            ROOT,
            0,
            1,
            new RuntimeMembershipPayload(ImmutableList.of(Domain.singleValue(BIGINT, 17L)), ORDINARY));

    @Param({"16", "128", "1024"})
    public int size;

    @Param({"chain", "fanout", "reverse"})
    public String shape;

    private List<RuntimeConstraintSubscription> nodes;

    @Setup
    public void setup()
    {
        List<RuntimeConstraintSubscription> subscriptions = new ArrayList<>();
        RuntimeConstraintId previous = ROOT;
        for (int index = 0; index < size; index++) {
            RuntimeConstraintSubscription node = RuntimeConstraintSubscription.create(shape.equals("fanout") ? ROOT : previous, ROOT, "node " + index, IDENTITY);
            subscriptions.add(node);
            previous = node.id();
        }
        nodes = shape.equals("reverse") ? subscriptions.reversed() : subscriptions;
    }

    @Benchmark
    public RuntimeConstraintSnapshot graph()
    {
        try (RuntimeConstraintSubscriptions graph = new RuntimeConstraintSubscriptions(0, Long.MAX_VALUE, null, _ -> true, null)) {
            CompletableFuture<RuntimeConstraintSnapshot> input = new CompletableFuture<>();
            graph.registerInput(new RuntimeConstraintSubscription.Input(ROOT, ROOT), input);
            nodes.forEach(graph::register);
            List<CompletableFuture<RuntimeConstraintSnapshot>> results = nodes.stream().map(node -> graph.waitForUpdate(node.id(), 0)).toList();
            input.complete(VALUE);
            return results.getLast().join();
        }
    }
}
