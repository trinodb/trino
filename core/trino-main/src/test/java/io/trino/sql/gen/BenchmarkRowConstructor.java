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
import io.trino.SequencePageBuilder;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.operator.project.PageProjection;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Coalesce;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.planner.Symbol;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Optional;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;
import static java.util.Collections.nCopies;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.assertj.core.api.Assertions.assertThat;

@State(Scope.Thread)
@OutputTimeUnit(MICROSECONDS)
@Fork(2)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkRowConstructor
{
    private static final int POSITION_COUNT = 1024;

    @Param({"16", "32", "48", "64", "80"})
    private int fieldCount = 16;

    @Param({"bigint", "varchar"})
    private String type = "bigint";

    @Param({"direct", "all-computed", "mixed"})
    private String expressionShape = "direct";

    private PageProjection projection;
    private SourcePage inputPage;
    private SelectedPositions selectedPositions;

    @Setup
    public void setup()
    {
        Type fieldType = switch (type) {
            case "bigint" -> BIGINT;
            case "varchar" -> VARCHAR;
            default -> throw new IllegalArgumentException("Unsupported type: " + type);
        };

        String inputName = "$col_0";
        Symbol inputSymbol = new Symbol(fieldType, inputName);
        Expression input = new Reference(fieldType, inputName);
        Row row = new Row(createFields(input), RowType.anonymous(nCopies(fieldCount, fieldType)));

        TestingFunctionResolution functionResolution = new TestingFunctionResolution();
        projection = functionResolution.getPageFunctionCompiler()
                .compileProjection(row, ImmutableMap.of(inputSymbol, 0), SQL_STANDARD, Optional.empty())
                .get();

        Page page = SequencePageBuilder.createSequencePage(ImmutableList.of(fieldType), POSITION_COUNT);
        inputPage = projection.getInputChannels().getInputChannels(SourcePage.create(page));
        selectedPositions = SelectedPositions.positionsRange(0, POSITION_COUNT);
    }

    private List<Expression> createFields(Expression input)
    {
        Expression computedInput = new Coalesce(input, input);
        return switch (expressionShape) {
            case "direct" -> nCopies(fieldCount, input);
            case "all-computed" -> nCopies(fieldCount, computedInput);
            case "mixed" -> {
                ImmutableList.Builder<Expression> fields = ImmutableList.builderWithExpectedSize(fieldCount);
                for (int field = 0; field < fieldCount; field++) {
                    fields.add(field % 2 == 0 ? input : computedInput);
                }
                yield fields.build();
            }
            default -> throw new IllegalArgumentException("Unsupported expression shape: " + expressionShape);
        };
    }

    @Benchmark
    public Block project()
    {
        return projection.project(SESSION, inputPage, selectedPositions);
    }

    @Test
    void testBenchmark()
    {
        for (String expressionShape : ImmutableList.of("direct", "all-computed", "mixed")) {
            this.expressionShape = expressionShape;
            for (String type : ImmutableList.of("bigint", "varchar")) {
                this.type = type;
                for (int fieldCount : ImmutableList.of(16, 32, 48, 64, 80)) {
                    this.fieldCount = fieldCount;
                    setup();
                    assertThat(project().getPositionCount()).isEqualTo(POSITION_COUNT);
                }
            }
        }
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkRowConstructor.class)
                .withOptions(options -> options.addProfiler(GCProfiler.class))
                .run();
    }
}
