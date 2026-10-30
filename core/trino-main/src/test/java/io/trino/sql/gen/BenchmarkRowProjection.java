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
package io.trino.sql.gen;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
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
import org.openjdk.jmh.runner.RunnerException;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;

@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class BenchmarkRowProjection
{
    private static final int POSITIONS = 1024;

    private PageProjection projection;
    private SourcePage input;

    @Setup
    public void setup()
    {
        Reference a = new Reference(BIGINT, "a");
        Reference b = new Reference(BIGINT, "b");
        Reference c = new Reference(BIGINT, "c");
        Row row = new Row(ImmutableList.of(a, new Row(ImmutableList.of(b, c))));
        projection = new TestingFunctionResolution().getPageFunctionCompiler()
                .compileProjection(row, ImmutableMap.of(new Symbol(BIGINT, "a"), 0, new Symbol(BIGINT, "b"), 1, new Symbol(BIGINT, "c"), 2), SQL_STANDARD, Optional.empty())
                .get();
        BlockBuilder builder = BIGINT.createBlockBuilder(null, POSITIONS);
        for (int position = 0; position < POSITIONS; position++) {
            BIGINT.writeLong(builder, position);
        }
        Block block = builder.build();
        input = projection.getInputChannels().getInputChannels(SourcePage.create(new Page(block, block, block)));
    }

    @Benchmark
    public Block project()
    {
        return projection.project(SESSION, input, SelectedPositions.positionsRange(0, POSITIONS));
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkRowProjection.class).run();
    }
}
