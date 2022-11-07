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
package io.trino.benchmark;

import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import io.trino.operator.FilterAndProjectOperator;
import io.trino.operator.OperatorFactory;
import io.trino.operator.project.PageProcessor;
import io.trino.sql.gen.ExpressionCompiler;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.plan.PlanNodeId;
import io.trino.sql.relational.RowExpression;
import io.trino.testing.LocalQueryRunner;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.sql.relational.Expressions.call;
import static io.trino.sql.relational.Expressions.constant;
import static io.trino.sql.relational.Expressions.field;

public class PredicateFilterBenchmark
        extends AbstractSimpleOperatorBenchmark
{
    public PredicateFilterBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "predicate_filter", 5, 50);
    }

    @Override
    protected List<? extends OperatorFactory> createOperatorFactories()
    {
        OperatorFactory tableScanOperator = createTableScanOperator(0, new PlanNodeId("test"), "orders", "totalprice");
        RowExpression filter = call(
                localQueryRunner.getMetadata().resolveOperator(session, LESS_THAN_OR_EQUAL, ImmutableList.of(DOUBLE, DOUBLE)),
                constant(50000.0, DOUBLE),
                field(0, DOUBLE));
        ExpressionCompiler expressionCompiler = new ExpressionCompiler(localQueryRunner.getFunctionManager(), new PageFunctionCompiler(localQueryRunner.getFunctionManager(), 0));
        Supplier<PageProcessor> pageProcessor = expressionCompiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, DOUBLE)));

        OperatorFactory filterAndProjectOperator = FilterAndProjectOperator.createOperatorFactory(
                1,
                new PlanNodeId("test"),
                pageProcessor,
                ImmutableList.of(DOUBLE),
                DataSize.ofBytes(0),
                0);

        return ImmutableList.of(tableScanOperator, filterAndProjectOperator);
    }

    public static void main(String[] args)
    {
        new PredicateFilterBenchmark(createLocalQueryRunner()).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}
