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
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.relational.SpecialForm.Form.OR;

public final class OrFilterEvaluator
        implements FilterEvaluator
{
    public static Optional<Supplier<FilterEvaluator>> createOrExpressionEvaluator(ColumnarFilterCompiler compiler, SpecialForm specialForm, boolean filterReorderingEnabled)
    {
        checkArgument(specialForm.form() == OR, "specialForm %s should be OR", specialForm);
        checkArgument(specialForm.arguments().size() >= 2, "OR expression %s should have at least 2 arguments", specialForm);

        ImmutableList.Builder<Supplier<FilterEvaluator>> builder = ImmutableList.builder();
        for (RowExpression expression : specialForm.arguments()) {
            Optional<Supplier<FilterEvaluator>> subExpressionEvaluator = FilterEvaluator.createColumnarFilterEvaluator(expression, compiler, filterReorderingEnabled);
            if (subExpressionEvaluator.isEmpty()) {
                return Optional.empty();
            }
            builder.add(subExpressionEvaluator.get());
        }
        List<Supplier<FilterEvaluator>> subExpressionEvaluators = builder.build();
        return Optional.of(() -> new OrFilterEvaluator(
                subExpressionEvaluators.stream().map(Supplier::get).collect(toImmutableList()),
                filterReorderingEnabled));
    }

    private final List<FilterEvaluator> subFilterEvaluators;
    private final FilterReorderingProfiler profiler;

    private OrFilterEvaluator(List<FilterEvaluator> subFilterEvaluators, boolean filterReorderingEnabled)
    {
        checkArgument(subFilterEvaluators.size() >= 2, "must have at least 2 subexpressions to OR");
        this.subFilterEvaluators = subFilterEvaluators;
        this.profiler = new FilterReorderingProfiler(subFilterEvaluators.size(), filterReorderingEnabled);
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        int inputPositions = activePositions.size();
        long filterTimeNanos = 0;
        SelectionResult result = getFirst().evaluate(session, activePositions, page);
        SelectedPositions accumulatedPositions = result.selectedPositions();
        profiler.addFilterMetrics(0, result.filterTimeNanos(), accumulatedPositions.size());
        filterTimeNanos += result.filterTimeNanos();
        activePositions = activePositions.difference(accumulatedPositions);
        for (int index = 1; index < subFilterEvaluators.size() - 1; index++) {
            int filterIndex = getFilterIndex(index);
            FilterEvaluator evaluator = subFilterEvaluators.get(filterIndex);
            result = evaluator.evaluate(session, activePositions, page);
            SelectedPositions selectedPositions = result.selectedPositions();
            profiler.addFilterMetrics(filterIndex, result.filterTimeNanos(), selectedPositions.size());
            filterTimeNanos += result.filterTimeNanos();
            accumulatedPositions = accumulatedPositions.union(selectedPositions);
            activePositions = activePositions.difference(selectedPositions);
        }
        int filterIndex = getFilterIndex(subFilterEvaluators.size() - 1);
        result = subFilterEvaluators.get(filterIndex).evaluate(session, activePositions, page);
        profiler.addFilterMetrics(filterIndex, result.filterTimeNanos(), result.selectedPositions().size());
        profiler.reorderFilters(inputPositions);
        filterTimeNanos += result.filterTimeNanos();
        accumulatedPositions = accumulatedPositions.union(result.selectedPositions());
        return new SelectionResult(accumulatedPositions, filterTimeNanos);
    }

    private int getFilterIndex(int index)
    {
        return profiler.getFilterOrder()[index];
    }

    private FilterEvaluator getFirst()
    {
        return subFilterEvaluators.get(getFilterIndex(0));
    }
}
