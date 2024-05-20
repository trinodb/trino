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
package io.trino.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import io.trino.matching.Captures;
import io.trino.matching.Pattern;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Comparison;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.iterative.Rule;
import io.trino.sql.planner.plan.Assignments;
import io.trino.sql.planner.plan.ExceptNode;
import io.trino.sql.planner.plan.FilterNode;
import io.trino.sql.planner.plan.ProjectNode;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.ir.Comparison.Operator.LESS_THAN_OR_EQUAL;
import static io.trino.sql.planner.plan.Patterns.Except.distinct;
import static io.trino.sql.planner.plan.Patterns.except;
import static java.util.Objects.requireNonNull;

/**
 * Implement EXCEPT ALL using union, window and filter.
 * <p>
 * Transforms:
 * <pre>{@code
 * - Except all
 *   output: a, b
 *     - Source1 (a1, b1)
 *     - Source2 (a2, b2)
 *     - Source3 (a3, b3)
 * }</pre>
 * Into:
 * <pre>{@code
 * - Project (prune helper symbols)
 *   output: a, b
 *     - Filter (row_number <= greatest(greatest(count1 - count2, 0) - count3, 0))
 *         - Window (partition by a, b)
 *           count1 <- count(marker1)
 *           count2 <- count(marker2)
 *           count3 <- count(marker3)
 *           row_number <- row_number()
 *               - Union
 *                 output: a, b, marker1, marker2, marker3
 *                   - Project (marker1 <- true, marker2 <- null, marker3 <- null)
 *                       - Source1 (a1, b1)
 *                   - Project (marker1 <- null, marker2 <- true, marker3 <- null)
 *                       - Source2 (a2, b2)
 *                   - Project (marker1 <- null, marker2 <- null, marker3 <- true)
 *                       - Source3 (a3, b3)
 * }</pre>
 */
public class ImplementExceptAll
        implements Rule<ExceptNode>
{
    private static final Pattern<ExceptNode> PATTERN = except()
            .with(distinct().equalTo(false));

    private final Metadata metadata;

    public ImplementExceptAll(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<ExceptNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(ExceptNode node, Captures captures, Context context)
    {
        SetOperationNodeTranslator translator = new SetOperationNodeTranslator(context.getSession(), metadata, context.getSymbolAllocator(), context.getIdAllocator());
        SetOperationNodeTranslator.TranslationResult result = translator.makeSetContainmentPlanForAll(node);

        // compute expected multiplicity for every row
        checkState(result.getCountSymbols().size() > 0, "ExceptNode translation result has no count symbols");
        ResolvedFunction greatest = metadata.resolveBuiltinFunction("greatest", fromTypes(BIGINT, BIGINT));

        Expression count = result.getCountSymbols().get(0).toSymbolReference();
        for (int i = 1; i < result.getCountSymbols().size(); i++) {
            count = new Call(
                    greatest,
                    ImmutableList.of(
                            new Call(
                                    metadata.resolveOperator(OperatorType.SUBTRACT, ImmutableList.of(BIGINT, BIGINT)),
                                    ImmutableList.of(count, result.getCountSymbols().get(i).toSymbolReference())),
                            new Constant(BIGINT, 0L)));
        }

        // filter rows so that expected number of rows remains
        Expression removeExtraRows = new Comparison(LESS_THAN_OR_EQUAL, result.getRowNumberSymbol().toSymbolReference(), count);
        FilterNode filter = new FilterNode(
                context.getIdAllocator().getNextId(),
                result.getPlanNode(),
                removeExtraRows);

        // prune helper symbols
        ProjectNode project = new ProjectNode(
                context.getIdAllocator().getNextId(),
                filter,
                Assignments.identity(node.getOutputSymbols()));

        return Result.ofPlanNode(project);
    }
}
