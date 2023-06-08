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
package io.trino.sql.gen.columnar;

import com.google.common.collect.ImmutableList;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.relational.SpecialForm.Form.OR;

public final class OrExpressionEvaluator
        implements ExpressionEvaluator
{
    public static Optional<Supplier<ExpressionEvaluator>> createOrExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.getForm() == OR, "specialForm %s should be OR", specialForm);
        checkArgument(specialForm.getArguments().size() >= 2, "OR expression %s should have at least 2 arguments", specialForm);

        ImmutableList.Builder<Supplier<ExpressionEvaluator>> builder = ImmutableList.builder();
        for (RowExpression expression : specialForm.getArguments()) {
            Optional<Supplier<ExpressionEvaluator>> subExpressionEvaluator = ExpressionEvaluator.createColumnarFilterEvaluator(expression, compiler);
            if (subExpressionEvaluator.isEmpty()) {
                return Optional.empty();
            }
            builder.add(subExpressionEvaluator.get());
        }
        List<Supplier<ExpressionEvaluator>> subExpressionEvaluators = builder.build();
        return Optional.of(() -> new OrExpressionEvaluator(subExpressionEvaluators.stream().map(Supplier::get).collect(toImmutableList())));
    }

    private final List<ExpressionEvaluator> subExpressionEvaluators;

    private OrExpressionEvaluator(List<ExpressionEvaluator> subExpressionEvaluators)
    {
        checkArgument(subExpressionEvaluators.size() >= 2, "must have at least 2 subexpressions to OR");
        this.subExpressionEvaluators = subExpressionEvaluators;
    }

    @Override
    public SelectedPositions evaluate(SelectedPositions activePositions, Page page)
    {
        SelectedPositions accumulatedPositions = null;
        for (int index = 0; index < subExpressionEvaluators.size(); index++) {
            ExpressionEvaluator evaluator = subExpressionEvaluators.get(index);
            SelectedPositions selectedPositions = evaluator.evaluate(activePositions, page);
            accumulatedPositions = accumulatedPositions == null ? selectedPositions : accumulatedPositions.union(selectedPositions);
            if (index != subExpressionEvaluators.size() - 1) {
                activePositions = activePositions.difference(selectedPositions);
            }
        }
        return accumulatedPositions;
    }
}
