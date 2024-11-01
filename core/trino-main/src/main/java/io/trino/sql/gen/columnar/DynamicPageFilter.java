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

import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.Session;
import io.trino.metadata.Metadata;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.sql.PlannerContext;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.optimizer.IrExpressionOptimizer;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import jakarta.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.ir.optimizer.IrExpressionOptimizer.newOptimizer;
import static io.trino.sql.relational.SqlToRowExpressionTranslator.translate;
import static java.util.Objects.requireNonNull;

public final class DynamicPageFilter
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final Session session;
    private final IrExpressionOptimizer irExpressionOptimizer;
    private final DomainTranslator domainTranslator;
    private final Map<ColumnHandle, Symbol> columnHandles;
    private final Map<Symbol, Integer> sourceLayout;
    private final double selectivityThreshold;

    @Nullable
    @GuardedBy("this")
    private Supplier<FilterEvaluator> compiledDynamicFilter;
    @Nullable
    @GuardedBy("this")
    private CompletableFuture<?> isBlocked;
    @Nullable
    @GuardedBy("this")
    private DynamicFilter currentDynamicFilter;

    public DynamicPageFilter(
            PlannerContext plannerContext,
            Session session,
            Map<Symbol, ColumnHandle> columnHandles,
            Map<Symbol, Integer> sourceLayout,
            double selectivityThreshold)
    {
        this.metadata = requireNonNull(plannerContext.getMetadata(), "metadata is null");
        this.typeManager = requireNonNull(plannerContext.getTypeManager(), "typeManager is null");
        this.session = requireNonNull(session, "session is null");
        this.irExpressionOptimizer = newOptimizer(plannerContext);
        this.domainTranslator = new DomainTranslator(plannerContext.getMetadata());
        this.columnHandles = columnHandles.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));
        this.sourceLayout = ImmutableMap.copyOf(sourceLayout);
        this.selectivityThreshold = selectivityThreshold;
    }

    // Compiled dynamic filter is fixed per-split and generated duration page source creation.
    // Page source implementations may subsequently implement blocking on completion of dynamic filters, but since
    // that occurs after page source creation, we cannot be guaranteed a completed dynamic filter here for initial splits
    public synchronized Supplier<FilterEvaluator> createDynamicPageFilterEvaluator(ColumnarFilterCompiler compiler, DynamicFilter dynamicFilter)
    {
        requireNonNull(dynamicFilter, "dynamicFilter is null");
        // Sub-query cache may provide different instance of DynamicFilter per-split.
        if (!dynamicFilter.equals(currentDynamicFilter)) {
            compiledDynamicFilter = null;
            currentDynamicFilter = dynamicFilter;
            isBlocked = dynamicFilter.isBlocked();
        }
        if (isBlocked == null) {
            return compiledDynamicFilter;
        }
        // Recompile dynamic filter if it has potentially changed since last compilation
        // We will re-compile each time DynamicFilter unblocks which should be equal to number of dynamic filters
        if (compiledDynamicFilter == null || isBlocked.isDone()) {
            isBlocked = dynamicFilter.isBlocked();
            boolean isAwaitable = dynamicFilter.isAwaitable();
            TupleDomain<Symbol> currentPredicate = dynamicFilter.getCurrentPredicate().transformKeys(columnHandles::get);
            List<Expression> expressionConjuncts = domainTranslator.toPredicateConjuncts(currentPredicate)
                    .stream()
                    // Run the expression derived from TupleDomain through IR optimizer to simplify predicates. E.g. SimplifyContinuousInValues
                    .map(expression -> irExpressionOptimizer.process(expression, session, ImmutableMap.of()).orElse(expression))
                    .collect(toImmutableList());
            // We translate each conjunct into separate RowExpression to make it easy to profile selectivity
            // of dynamic filter per column and drop them if they're ineffective
            List<RowExpression> rowExpression = expressionConjuncts.stream()
                    .map(expression -> translate(expression, sourceLayout, metadata, typeManager))
                    .collect(toImmutableList());
            compiledDynamicFilter = createDynamicFilterEvaluator(rowExpression, compiler, selectivityThreshold);
            if (!isAwaitable) {
                isBlocked = null; // Dynamic filter will not narrow down anymore
            }
        }
        return compiledDynamicFilter;
    }

    private static Supplier<FilterEvaluator> createDynamicFilterEvaluator(List<RowExpression> rowExpressions, ColumnarFilterCompiler compiler, double selectivityThreshold)
    {
        List<Supplier<FilterEvaluator>> subExpressionEvaluators = rowExpressions.stream()
                .map(expression -> createColumnarFilterEvaluator(expression, compiler))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList());
        return () -> new DynamicFilterEvaluator(
                subExpressionEvaluators.stream().map(Supplier::get).collect(toImmutableList()),
                selectivityThreshold);
    }

    static final class DynamicFilterEvaluator
            implements FilterEvaluator
    {
        private final List<FilterEvaluator> subFilterEvaluators;
        private final EffectiveFilterProfiler profiler;

        private DynamicFilterEvaluator(List<FilterEvaluator> subFilterEvaluators, double selectivityThreshold)
        {
            this.subFilterEvaluators = subFilterEvaluators;
            this.profiler = new EffectiveFilterProfiler(selectivityThreshold, subFilterEvaluators.size());
        }

        @Override
        public SelectionResult evaluate(ConnectorSession session, SelectedPositions activePositions, SourcePage page)
        {
            long filterTimeNanos = 0;
            for (int filterIndex = 0; filterIndex < subFilterEvaluators.size(); filterIndex++) {
                if (!profiler.isFilterEffective(filterIndex)) {
                    continue;
                }
                FilterEvaluator evaluator = subFilterEvaluators.get(filterIndex);
                SelectionResult result = evaluator.evaluate(session, activePositions, page);
                profiler.recordSelectivity(activePositions, result.selectedPositions(), filterIndex);
                filterTimeNanos += result.filterTimeNanos();
                activePositions = result.selectedPositions();
            }
            return new SelectionResult(activePositions, filterTimeNanos);
        }
    }

    private static class EffectiveFilterProfiler
    {
        private static final int MIN_SAMPLE_POSITIONS = 2047;

        private final double selectivityThreshold;
        private final long[] filterInputPositions;
        private final long[] filterOutputPositions;
        private final IntOpenHashSet ineffectiveFilterChannels;

        private EffectiveFilterProfiler(double selectivityThreshold, int filterCount)
        {
            this.selectivityThreshold = selectivityThreshold;
            this.filterInputPositions = new long[filterCount];
            this.filterOutputPositions = new long[filterCount];
            this.ineffectiveFilterChannels = new IntOpenHashSet(filterCount);
        }

        private void recordSelectivity(SelectedPositions inputPositions, SelectedPositions selectedPositions, int filterIndex)
        {
            filterInputPositions[filterIndex] += inputPositions.size();
            filterOutputPositions[filterIndex] += selectedPositions.size();
            if (filterInputPositions[filterIndex] < MIN_SAMPLE_POSITIONS) {
                return;
            }
            if (filterOutputPositions[filterIndex] > (selectivityThreshold * filterInputPositions[filterIndex])) {
                ineffectiveFilterChannels.add(filterIndex);
            }
        }

        private boolean isFilterEffective(int filterIndex)
        {
            return !ineffectiveFilterChannels.contains(filterIndex);
        }
    }
}
