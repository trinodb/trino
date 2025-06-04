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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.trino.operator.project.CursorProcessor;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.PageProcessor;
import io.trino.operator.project.PageProjection;
import io.trino.spi.connector.DynamicFilter;
import io.trino.sql.gen.columnar.ColumnarFilterCompiler;
import io.trino.sql.gen.columnar.DynamicPageFilter;
import io.trino.sql.gen.columnar.FilterEvaluator;
import io.trino.sql.gen.columnar.PageFilterEvaluator;
import io.trino.sql.relational.RowExpression;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static java.util.Objects.requireNonNull;

public class ExpressionCompiler
{
    private final CursorProcessorCompiler cursorProcessorCompiler;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final ColumnarFilterCompiler columnarFilterCompiler;

    @Inject
    public ExpressionCompiler(CursorProcessorCompiler cursorProcessorCompiler, PageFunctionCompiler pageFunctionCompiler, ColumnarFilterCompiler columnarFilterCompiler)
    {
        this.cursorProcessorCompiler = requireNonNull(cursorProcessorCompiler, "cursorProcessorCompiler is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "pageFunctionCompiler is null");
        this.columnarFilterCompiler = requireNonNull(columnarFilterCompiler, "columnarFilterCompiler is null");
    }

    public Supplier<CursorProcessor> compileCursorProcessor(Optional<RowExpression> filter, List<RowExpression> projections, Object uniqueKey)
    {
        return cursorProcessorCompiler.compileCursorProcessor(filter, projections, uniqueKey);
    }

    public Function<DynamicFilter, PageProcessor> compilePageProcessor(
            boolean columnarFilterEvaluationEnabled,
            Optional<RowExpression> filter,
            Optional<DynamicPageFilter> dynamicPageFilter,
            List<? extends RowExpression> projections,
            Optional<String> classNameSuffix,
            OptionalInt initialBatchSize)
    {
        Optional<Supplier<PageFilter>> filterFunctionSupplier = Optional.empty();
        Optional<Supplier<FilterEvaluator>> columnarFilterEvaluatorSupplier = createColumnarFilterEvaluator(columnarFilterEvaluationEnabled, filter, columnarFilterCompiler);
        if (columnarFilterEvaluatorSupplier.isEmpty()) {
            filterFunctionSupplier = filter.map(expression -> pageFunctionCompiler.compileFilter(expression, classNameSuffix));
        }

        List<Supplier<PageProjection>> pageProjectionSuppliers = projections.stream()
                .map(projection -> pageFunctionCompiler.compileProjection(projection, classNameSuffix))
                .collect(toImmutableList());

        Optional<Supplier<PageFilter>> finalFilterFunctionSupplier = filterFunctionSupplier;
        return (dynamicFilter) -> {
            Optional<FilterEvaluator> filterEvaluator = columnarFilterEvaluatorSupplier.map(Supplier::get);
            if (filterEvaluator.isEmpty()) {
                filterEvaluator = finalFilterFunctionSupplier
                        .map(Supplier::get)
                        .map(PageFilterEvaluator::new);
            }
            List<PageProjection> pageProjections = pageProjectionSuppliers.stream()
                    .map(Supplier::get)
                    .collect(toImmutableList());
            Optional<FilterEvaluator> dynamicFilterEvaluator = dynamicPageFilter
                    .map(pageFilter -> pageFilter.createDynamicPageFilterEvaluator(columnarFilterCompiler, dynamicFilter))
                    .map(Supplier::get);
            return new PageProcessor(filterEvaluator, dynamicFilterEvaluator, pageProjections, initialBatchSize);
        };
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections)
    {
        return () -> compilePageProcessor(true, filter, Optional.empty(), projections, Optional.empty(), OptionalInt.empty())
                .apply(DynamicFilter.EMPTY);
    }

    @VisibleForTesting
    public Supplier<PageProcessor> compilePageProcessor(Optional<RowExpression> filter, List<? extends RowExpression> projections, int initialBatchSize)
    {
        return () -> compilePageProcessor(true, filter, Optional.empty(), projections, Optional.empty(), OptionalInt.of(initialBatchSize))
                .apply(DynamicFilter.EMPTY);
    }
}
