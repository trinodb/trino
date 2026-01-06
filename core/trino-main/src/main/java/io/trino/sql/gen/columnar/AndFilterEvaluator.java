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
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.sql.relational.SpecialForm.Form.AND;
import static java.util.Objects.requireNonNull;

public final class AndFilterEvaluator
        implements FilterEvaluator
{
    public static Optional<Supplier<FilterEvaluator>> createAndExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm)
    {
        checkArgument(specialForm.form() == AND, "specialForm %s should be AND", specialForm);

        List<RowExpression> arguments = specialForm.arguments();
        checkArgument(arguments.size() >= 2, "AND expression %s should have at least 2 arguments", specialForm);

        ImmutableList.Builder<Supplier<FilterEvaluator>> builder = ImmutableList.builderWithExpectedSize(arguments.size());
        for (RowExpression expression : arguments) {
            Optional<Supplier<FilterEvaluator>> subExpressionEvaluator = FilterEvaluator.createColumnarFilterEvaluator(expression, compiler);
            if (subExpressionEvaluator.isEmpty()) {
                return Optional.empty();
            }
            builder.add(subExpressionEvaluator.get());
        }
        List<Supplier<FilterEvaluator>> subExpressionEvaluators = builder.build();
        return Optional.of(() -> {
            FilterEvaluator[] filterEvaluators = new FilterEvaluator[subExpressionEvaluators.size()];
            for (int i = 0; i < filterEvaluators.length; i++) {
                filterEvaluators[i] = requireNonNull(subExpressionEvaluators.get(i).get(), "subExpressionEvaluator is null");
            }
            return new AndFilterEvaluator(filterEvaluators);
        });
    }

    private final FilterEvaluator[] subFilterEvaluators;

    private AndFilterEvaluator(FilterEvaluator[] subFilterEvaluators)
    {
        checkArgument(subFilterEvaluators.length >= 2, "must have at least 2 subexpressions to AND");
        this.subFilterEvaluators = subFilterEvaluators;
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        long filterTimeNanos = 0;
        for (FilterEvaluator evaluator : subFilterEvaluators) {
            if (activePositions.isEmpty()) {
                break;
            }
            SelectionResult result = evaluator.evaluate(session, activePositions, page);
            filterTimeNanos += result.filterTimeNanos();
            activePositions = result.selectedPositions();
        }
        return new SelectionResult(activePositions, filterTimeNanos);
    }
}
