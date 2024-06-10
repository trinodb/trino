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
import io.trino.metadata.Metadata;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.TypeManager;
import io.trino.sql.ir.Expression;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import jakarta.annotation.Nullable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.sql.gen.columnar.FilterEvaluator.createColumnarFilterEvaluator;
import static io.trino.sql.relational.SqlToRowExpressionTranslator.translate;
import static java.util.Objects.requireNonNull;

public final class DynamicPageFilter
{
    private final Metadata metadata;
    private final TypeManager typeManager;
    private final DomainTranslator domainTranslator;
    private final DynamicFilter dynamicFilter;
    private final Map<ColumnHandle, Symbol> columnHandles;
    private final Map<Symbol, Integer> sourceLayout;

    @Nullable
    @GuardedBy("this")
    private Supplier<FilterEvaluator> compiledDynamicFilter;
    @Nullable
    @GuardedBy("this")
    private CompletableFuture<?> isBlocked;

    public DynamicPageFilter(
            Metadata metadata,
            TypeManager typeManager,
            DynamicFilter dynamicFilter,
            Map<Symbol, ColumnHandle> columnHandles,
            Map<Symbol, Integer> sourceLayout)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.domainTranslator = new DomainTranslator(metadata);
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.columnHandles = columnHandles.entrySet()
                .stream()
                .collect(toImmutableMap(Map.Entry::getValue, Map.Entry::getKey));
        this.sourceLayout = ImmutableMap.copyOf(sourceLayout);
        this.isBlocked = dynamicFilter.isBlocked();
    }

    // Compiled dynamic filter is fixed per-split and generated duration page source creation.
    // Page source implementations may subsequently implement blocking on completion of dynamic filters, but since
    // that occurs after page source creation, we cannot be guaranteed a completed dynamic filter here for initial splits
    public synchronized Supplier<FilterEvaluator> createDynamicPageFilterEvaluator(ColumnarFilterCompiler compiler)
    {
        if (isBlocked == null) {
            return compiledDynamicFilter;
        }
        // Recompile dynamic filter if it has potentially changed since last compilation
        // We will re-compile each time DynamicFilter unblocks which should be equal to number of dynamic filters
        if (compiledDynamicFilter == null || isBlocked.isDone()) {
            isBlocked = dynamicFilter.isBlocked();
            boolean isAwaitable = dynamicFilter.isAwaitable();
            TupleDomain<Symbol> currentPredicate = dynamicFilter.getCurrentPredicate().transformKeys(columnHandles::get);
            Expression expression = domainTranslator.toPredicate(currentPredicate);
            RowExpression rowExpression = translate(expression, sourceLayout, metadata, typeManager);
            compiledDynamicFilter = createColumnarFilterEvaluator(rowExpression, compiler).orElse(SelectAllEvaluator::new);
            if (!isAwaitable) {
                isBlocked = null; // Dynamic filter will not narrow down anymore
            }
        }
        return compiledDynamicFilter;
    }
}
