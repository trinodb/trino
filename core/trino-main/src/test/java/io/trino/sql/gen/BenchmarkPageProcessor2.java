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
import io.trino.Session;
import io.trino.metadata.FunctionManager;
import io.trino.operator.DriverYieldSignal;
import io.trino.operator.index.PageRecordSet;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageProcessor;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.Type;
import io.trino.sql.planner.Symbol;
import io.trino.sql.planner.TypeAnalyzer;
import io.trino.sql.planner.TypeProvider;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SqlToRowExpressionTranslator;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.NodeRef;
import io.trino.testing.TestingSession;
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
import org.openjdk.jmh.runner.RunnerException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static io.trino.jmh.Benchmarks.benchmark;
import static io.trino.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static io.trino.metadata.FunctionManager.createTestingFunctionManager;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.ExpressionTestUtils.createExpression;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.sql.planner.TypeAnalyzer.createTestingTypeAnalyzer;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.toList;

@SuppressWarnings({"PackageVisibleField", "FieldCanBeLocal"})
@State(Scope.Thread)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(10)
@Warmup(iterations = 10)
@Measurement(iterations = 10)
@BenchmarkMode(Mode.AverageTime)
public class BenchmarkPageProcessor2
{
    private static final Map<String, Type> TYPE_MAP = ImmutableMap.of("bigint", BIGINT, "varchar", VARCHAR);
    private static final TypeAnalyzer TYPE_ANALYZER = createTestingTypeAnalyzer(PLANNER_CONTEXT);
    private static final Session TEST_SESSION = TestingSession.testSessionBuilder().build();
    private static final int POSITIONS = 1024;

    private final DriverYieldSignal yieldSignal = new DriverYieldSignal();
    private final Map<Symbol, Type> symbolTypes = new HashMap<>();
    private final Map<Symbol, Integer> sourceLayout = new HashMap<>();

    private CursorProcessor cursorProcessor;
    private PageProcessor pageProcessor;
    private Page inputPage;
    private RecordSet recordSet;
    private List<Type> types;

    @Param({"2", "4", "8", "16", "32"})
    int columnCount;

    @Param({"varchar", "bigint"})
    String type;

    @Param({"false", "true"})
    boolean dictionaryBlocks;

    @Setup
    public void setup()
    {
        Type type = TYPE_MAP.get(this.type);

        for (int i = 0; i < columnCount; i++) {
            Symbol symbol = new Symbol(type.getDisplayName().toLowerCase(ENGLISH) + i);
            symbolTypes.put(symbol, type);
            sourceLayout.put(symbol, i);
        }

        List<RowExpression> projections = getProjections(type);
        types = projections.stream().map(RowExpression::getType).collect(toList());

        FunctionManager functionManager = createTestingFunctionManager();
        PageFunctionCompiler pageFunctionCompiler = new PageFunctionCompiler(functionManager, 0);

        inputPage = createPage(types, dictionaryBlocks);
        pageProcessor = new ExpressionCompiler(functionManager, pageFunctionCompiler).compilePageProcessor(Optional.of(getFilter(type)), projections).get();

        recordSet = new PageRecordSet(types, inputPage);
        cursorProcessor = new ExpressionCompiler(functionManager, pageFunctionCompiler).compileCursorProcessor(Optional.of(getFilter(type)), projections, "key").get();
    }

    @Benchmark
    public Page rowOriented()
    {
        PageBuilder pageBuilder = new PageBuilder(types);
        cursorProcessor.process(null, yieldSignal, recordSet.cursor(), pageBuilder);
        return pageBuilder.build();
    }

    @Benchmark
    public List<Optional<Page>> columnOriented()
    {
        return ImmutableList.copyOf(
                pageProcessor.process(
                        null,
                        new DriverYieldSignal(),
                        newSimpleAggregatedMemoryContext().newLocalMemoryContext(PageProcessor.class.getSimpleName()),
                        inputPage));
    }

    private RowExpression getFilter(Type type)
    {
        if (type == VARCHAR) {
            return rowExpression("cast(varchar0 as bigint) % 2 = 0");
        }
        if (type == BIGINT) {
            return rowExpression("bigint0 % 2 = 0");
        }
        throw new IllegalArgumentException("filter not supported for type : " + type);
    }

    private List<RowExpression> getProjections(Type type)
    {
        ImmutableList.Builder<RowExpression> builder = ImmutableList.builder();
        if (type == BIGINT) {
            for (int i = 0; i < columnCount; i++) {
                builder.add(rowExpression("bigint" + i + " + 5"));
            }
        }
        else if (type == VARCHAR) {
            for (int i = 0; i < columnCount; i++) {
                // alternatively use identity expression rowExpression("varchar" + i, type) or
                // rowExpression("substr(varchar" + i + ", 1, 1)", type)
                builder.add(rowExpression("concat(varchar" + i + ", 'foo')"));
            }
        }
        return builder.build();
    }

    private RowExpression rowExpression(String value)
    {
        Expression expression = createExpression(value, PLANNER_CONTEXT, TypeProvider.copyOf(symbolTypes));

        Map<NodeRef<Expression>, Type> expressionTypes = TYPE_ANALYZER.getTypes(TEST_SESSION, TypeProvider.copyOf(symbolTypes), expression);
        return SqlToRowExpressionTranslator.translate(
                expression,
                expressionTypes,
                sourceLayout,
                PLANNER_CONTEXT.getMetadata(),
                PLANNER_CONTEXT.getFunctionManager(),
                TEST_SESSION,
                true);
    }

    private static Page createPage(List<? extends Type> types, boolean dictionary)
    {
        if (dictionary) {
            return SequencePageBuilder.createSequencePageWithDictionaryBlocks(types, POSITIONS);
        }
        else {
            return SequencePageBuilder.createSequencePage(types, POSITIONS);
        }
    }

    public static void main(String[] args)
            throws RunnerException
    {
        benchmark(BenchmarkPageProcessor2.class).run();
    }
}
