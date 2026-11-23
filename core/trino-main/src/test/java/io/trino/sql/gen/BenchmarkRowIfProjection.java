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
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Case;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.IsNull;
import io.trino.sql.ir.Reference;
import io.trino.sql.ir.Row;
import io.trino.sql.ir.WhenClause;
import io.trino.sql.planner.Symbol;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static io.trino.type.CharVarcharCoercion.SQL_STANDARD;

/**
 * Projects a wide row column through ROW(...), IF(x IS NULL, NULL, ROW(...)) and the same IF at three nesting levels.
 */
@State(Scope.Thread)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 2)
@Measurement(iterations = 5, time = 2)
@OperationsPerInvocation(BenchmarkRowIfProjection.POSITIONS)
public class BenchmarkRowIfProjection
{
    static final int POSITIONS = 300_000;

    private static final ArrayType ARRAY = new ArrayType(VARCHAR);
    // fields: varchar, varchar, bigint, varchar, integer, [nested], array
    private static final RowType LEAF = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, BIGINT, VARCHAR, INTEGER, ARRAY));
    private static final RowType MIDDLE = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, BIGINT, VARCHAR, INTEGER, LEAF, ARRAY));
    private static final RowType TOP = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, BIGINT, VARCHAR, INTEGER, MIDDLE, ARRAY));
    private static final RowType FLAT_NESTED = RowType.anonymous(ImmutableList.of(VARCHAR, VARCHAR, BIGINT, VARCHAR, INTEGER, RowType.anonymous(ImmutableList.of(VARCHAR, BIGINT)), ARRAY));

    @Param({"row", "if", "if3"})
    public String projectionKind;

    private PageProjection projection;
    private SourcePage input;

    @Setup
    public void setup()
    {
        RowType type = projectionKind.equals("if3") ? TOP : FLAT_NESTED;
        Reference x = new Reference(type, "x");
        Expression expression = switch (projectionKind) {
            case "row" -> rebuild(x, false);
            case "if" -> rebuild(x, true);
            case "if3" -> rebuild(x, true);
            default -> throw new IllegalArgumentException(projectionKind);
        };
        projection = new TestingFunctionResolution().getPageFunctionCompiler()
                .compileProjection(expression, ImmutableMap.of(new Symbol(type, "x"), 0), SQL_STANDARD, Optional.empty())
                .get();

        RowBlockBuilder builder = (RowBlockBuilder) type.createBlockBuilder(null, POSITIONS);
        for (int position = 0; position < POSITIONS; position++) {
            if (position % 10 == 0) {
                builder.appendNull();
                continue;
            }
            int p = position;
            builder.buildEntry(fields -> writeRow(type, fields, p));
        }
        Block block = builder.build();
        input = projection.getInputChannels().getInputChannels(SourcePage.create(new Page(block)));
    }

    private static void writeRow(RowType type, List<BlockBuilder> fields, int position)
    {
        for (int i = 0; i < fields.size(); i++) {
            Type fieldType = type.getFields().get(i).getType();
            BlockBuilder field = fields.get(i);
            if (fieldType == VARCHAR) {
                VARCHAR.writeSlice(field, utf8Slice("value-" + position + "-" + i + "-padding"));
            }
            else if (fieldType == BIGINT) {
                BIGINT.writeLong(field, position);
            }
            else if (fieldType == INTEGER) {
                INTEGER.writeInt(field, position);
            }
            else if (fieldType instanceof ArrayType) {
                ((ArrayBlockBuilder) field).buildEntry(elements -> {
                    VARCHAR.writeSlice(elements, utf8Slice("a" + position));
                    VARCHAR.writeSlice(elements, utf8Slice("b" + position));
                });
            }
            else {
                RowType nested = (RowType) fieldType;
                if (position % 7 == 0) {
                    field.appendNull();
                }
                else {
                    ((RowBlockBuilder) field).buildEntry(nestedFields -> writeRow(nested, nestedFields, position));
                }
            }
        }
    }

    // ROW(x.0, x.1, 42, x.3, 7, <nested>, x.6): two computed fields, everything else is a reference to x
    private static Expression rebuild(Expression x, boolean guarded)
    {
        RowType type = (RowType) x.type();
        ImmutableList.Builder<Expression> items = ImmutableList.builder();
        for (int i = 0; i < type.getFields().size(); i++) {
            Type fieldType = type.getFields().get(i).getType();
            Expression field = new FieldReference(x, i);
            if (i == 2) {
                field = new Constant(BIGINT, 42L);
            }
            else if (i == 4) {
                field = new Constant(INTEGER, 7L);
            }
            else if (fieldType instanceof RowType nestedType && nestedType.getFields().size() > 2) {
                field = rebuild(field, guarded);
            }
            items.add(field);
        }
        Expression row = new Row(items.build(), type);
        if (!guarded) {
            return row;
        }
        return new Case(ImmutableList.of(new WhenClause(new IsNull(x), new Constant(type, null))), row);
    }

    @Benchmark
    public Block project()
    {
        return projection.project(SESSION, input, SelectedPositions.positionsRange(0, POSITIONS));
    }

    static void main()
            throws RunnerException
    {
        benchmark(BenchmarkRowIfProjection.class)
                .withOptions(options -> options.addProfiler("gc"))
                .run();
    }
}
