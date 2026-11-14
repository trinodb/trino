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
import io.trino.sql.gen.columnar.PageBoundsFilterEvaluator.Decision;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.Logical;
import io.trino.sql.planner.Symbol;
import io.trino.type.CharVarcharCoercion;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.sql.gen.columnar.FilterEvaluator.createDictionaryAwareEvaluator;
import static io.trino.sql.gen.columnar.FilterEvaluator.isReorderingSafe;
import static io.trino.sql.gen.columnar.PageBoundsFilterEvaluator.boundsAware;
import static java.util.Objects.requireNonNull;

public final class AndFilterEvaluator
        implements FilterEvaluator
{
    public static Optional<Supplier<FilterEvaluator>> createAndExpressionEvaluator(CharVarcharCoercion charVarcharCoercion, ColumnarFilterCompiler compiler, Logical logical, Map<Symbol, Integer> layout, boolean filterReorderingEnabled, boolean dynamicFilter)
    {
        checkArgument(logical.operator() == Logical.Operator.AND, "logical %s should be AND", logical);

        checkArgument(logical.terms().size() >= 2, "AND expression %s should have at least 2 arguments", logical);

        // both bounds of a range on one column become a single kernel, see SameColumnRange
        List<Expression> terms = SameColumnRange.fuseTerms(logical.terms());
        ImmutableList.Builder<Supplier<FilterEvaluator>> builder = ImmutableList.builderWithExpectedSize(terms.size());
        for (Expression expression : terms) {
            Optional<Supplier<FilterEvaluator>> subExpressionEvaluator;
            if (SameColumnRange.isFusedRange(expression)) {
                subExpressionEvaluator = createSameColumnRangeEvaluator(charVarcharCoercion, compiler, expression, layout, dynamicFilter);
            }
            else {
                subExpressionEvaluator = FilterEvaluator.createColumnarFilterEvaluator(charVarcharCoercion, expression, layout, compiler, filterReorderingEnabled, dynamicFilter);
            }
            if (subExpressionEvaluator.isEmpty()) {
                return Optional.empty();
            }
            builder.add(subExpressionEvaluator.get());
        }
        List<Supplier<FilterEvaluator>> subExpressionEvaluators = builder.build();
        if (subExpressionEvaluators.size() == 1) {
            return Optional.of(subExpressionEvaluators.get(0));
        }
        boolean reorderingSafe = filterReorderingEnabled && isReorderingSafe(compiler.getPlannerContext(), charVarcharCoercion, logical.terms());
        return Optional.of(() -> {
            FilterEvaluator[] filterEvaluators = new FilterEvaluator[subExpressionEvaluators.size()];
            for (int i = 0; i < filterEvaluators.length; i++) {
                filterEvaluators[i] = requireNonNull(subExpressionEvaluators.get(i).get(), "subExpressionEvaluator is null");
            }
            return new AndFilterEvaluator(filterEvaluators, reorderingSafe);
        });
    }

    // the fused range reads one channel, so it gets the same dictionary aware evaluation as a single comparison
    private static Optional<Supplier<FilterEvaluator>> createSameColumnRangeEvaluator(CharVarcharCoercion charVarcharCoercion, ColumnarFilterCompiler compiler, Expression range, Map<Symbol, Integer> layout, boolean dynamicFilter)
    {
        Optional<Supplier<ColumnarFilter>> compiledFilter = compiler.generateFilter(charVarcharCoercion, range, layout, dynamicFilter);
        return compiledFilter.map(filterSupplier -> boundsAware(range, layout, () -> createDictionaryAwareEvaluator(filterSupplier.get())));
    }

    private final FilterEvaluator[] subFilterEvaluators;
    private final PageBoundsFilterEvaluator[] boundsAwareEvaluators;
    private final FilterReorderingProfiler profiler;

    private AndFilterEvaluator(FilterEvaluator[] subFilterEvaluators, boolean filterReorderingEnabled)
    {
        checkArgument(subFilterEvaluators.length >= 2, "must have at least 2 subexpressions to AND");
        this.subFilterEvaluators = subFilterEvaluators;
        this.boundsAwareEvaluators = Arrays.stream(subFilterEvaluators)
                .filter(PageBoundsFilterEvaluator.class::isInstance)
                .map(PageBoundsFilterEvaluator.class::cast)
                .toArray(PageBoundsFilterEvaluator[]::new);
        this.profiler = new FilterReorderingProfiler(subFilterEvaluators.length, filterReorderingEnabled);
    }

    @Override
    public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
    {
        // A term that the page bounds prove to reject every position decides the conjunction before any term reads data
        long start = System.nanoTime();
        for (PageBoundsFilterEvaluator evaluator : boundsAwareEvaluators) {
            if (evaluator.decide(page) == Decision.SELECT_NONE) {
                return new SelectionResult(positionsRange(0, 0), System.nanoTime() - start);
            }
        }
        int inputPositions = activePositions.size();
        long filterTimeNanos = 0;
        for (int filterIndex : profiler.getFilterOrder()) {
            if (activePositions.isEmpty()) {
                break;
            }
            FilterEvaluator evaluator = subFilterEvaluators[filterIndex];
            SelectionResult result = evaluator.evaluate(session, activePositions, page);
            profiler.addFilterMetrics(filterIndex, result.filterTimeNanos(), activePositions.size() - result.selectedPositions().size());
            filterTimeNanos += result.filterTimeNanos();
            activePositions = result.selectedPositions();
        }
        profiler.reorderFilters(inputPositions);
        return new SelectionResult(activePositions, filterTimeNanos);
    }
}
