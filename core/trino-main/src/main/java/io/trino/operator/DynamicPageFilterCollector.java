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
package io.trino.operator;

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.trino.metadata.Metadata;
import io.trino.operator.project.PageFilter;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.sql.gen.PageFunctionCompiler;
import io.trino.sql.planner.DomainTranslator;
import io.trino.sql.planner.Symbol;
import io.trino.sql.relational.RowExpression;
import io.trino.sql.relational.SpecialForm;
import io.trino.util.RowExpressionConverter;

import javax.annotation.PreDestroy;
import javax.annotation.concurrent.GuardedBy;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toListenableFuture;
import static io.airlift.concurrent.Threads.threadsNamed;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

public class DynamicPageFilterCollector
{
    @GuardedBy("this")
    private TupleDomain<ColumnHandle> collectedDynamicFilter = TupleDomain.all();

    private final AtomicReference<Optional<Supplier<PageFilter>>> filterFunctionSupplier = new AtomicReference<>(Optional.empty());

    private final DynamicFilter dynamicFilter;
    private final Optional<RowExpression> staticFilter;
    private final Map<Symbol, ColumnHandle> columnHandles;
    private final RowExpressionConverter converter;
    private final DomainTranslator domainTranslator;
    private final PageFunctionCompiler pageFunctionCompiler;
    private final Optional<String> classNameSuffix;
    private final ExecutorService executorService;

    private DynamicPageFilterCollector(
            DomainTranslator domainTranslator,
            DynamicFilter dynamicFilter,
            Optional<RowExpression> staticFilter,
            Map<Symbol, ColumnHandle> columnHandles,
            RowExpressionConverter converter,
            PageFunctionCompiler pageFunctionCompiler,
            Optional<String> classNameSuffix)
    {
        this.domainTranslator = requireNonNull(domainTranslator, "domainTranslator is null");
        this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
        this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.converter = requireNonNull(converter, "converter is null");
        this.pageFunctionCompiler = requireNonNull(pageFunctionCompiler, "compiler is null");
        this.classNameSuffix = requireNonNull(classNameSuffix, "classNameSuffix is null");
        this.executorService = newSingleThreadExecutor(threadsNamed("dynamic-row-filter-collector-%s"));
    }

    private ListenableFuture<?> collectDynamicFilterAsync(SettableFuture<?> completionFuture)
    {
        if (dynamicFilter.isComplete()) {
            combineFilters();
            completionFuture.set(null);
        }
        else {
            toListenableFuture(dynamicFilter.isBlocked()).addListener(() -> {
                combineFilters();
                collectDynamicFilterAsync(completionFuture);
            }, executorService);
        }
        return completionFuture;
    }

    private synchronized void combineFilters()
    {
        TupleDomain<ColumnHandle> currentDynamicFilter = dynamicFilter.getCurrentPredicate();
        if (!currentDynamicFilter.equals(collectedDynamicFilter)) {
            collectedDynamicFilter = currentDynamicFilter;
            TupleDomain<Symbol> tupleDomain = translateSummaryIntoTupleDomain(collectedDynamicFilter, columnHandles);
            RowExpression combinedExpression = converter.toRowExpression(domainTranslator.toPredicate(tupleDomain));
            if (staticFilter.isPresent()) {
                combinedExpression = new SpecialForm(
                        SpecialForm.Form.AND,
                        BOOLEAN,
                        staticFilter.get(),
                        combinedExpression);
            }
            filterFunctionSupplier.set(Optional.of(pageFunctionCompiler.compileFilter(combinedExpression, classNameSuffix)));
        }
    }

    public void startCollectDynamicFilterAsync()
    {
        collectDynamicFilterAsync(SettableFuture.create())
                .addListener(this::cleanUp, directExecutor());
    }

    @PreDestroy
    private void cleanUp()
    {
        executorService.shutdownNow();
    }

    private static TupleDomain<Symbol> translateSummaryIntoTupleDomain(TupleDomain<ColumnHandle> summary, Map<Symbol, ColumnHandle> columnHandles)
    {
        if (summary.isNone()) {
            return TupleDomain.none();
        }
        if (summary.isAll()) {
            return TupleDomain.all();
        }

        Map<ColumnHandle, Domain> columnHandleDomains = summary.getDomains().get();
        ImmutableMap.Builder<Symbol, Domain> domainBuilder = ImmutableMap.builder();
        for (Map.Entry<Symbol, ColumnHandle> entry : columnHandles.entrySet()) {
            Domain domain = columnHandleDomains.get(entry.getValue());
            if (domain == null) {
                continue;
            }
            domainBuilder.put(entry.getKey(), domain);
        }
        return TupleDomain.withColumnDomains(domainBuilder.build());
    }

    public Optional<Supplier<PageFilter>> getFilterFunctionSupplier()
    {
        return filterFunctionSupplier.get();
    }

    public static class DynamicPageFilterCollectorFactory
    {
        private final DomainTranslator domainTranslator;
        private final DynamicFilter dynamicFilter;
        private final Optional<RowExpression> staticFilter;
        private final Map<Symbol, ColumnHandle> columnHandles;
        private final RowExpressionConverter converter;

        public DynamicPageFilterCollectorFactory(
                DynamicFilter dynamicFilter,
                Optional<RowExpression> staticFilter,
                Map<Symbol, ColumnHandle> columnHandles,
                RowExpressionConverter converter,
                Metadata metadata)
        {
            requireNonNull(metadata, "metadata is null");
            this.domainTranslator = new DomainTranslator(metadata);
            this.dynamicFilter = requireNonNull(dynamicFilter, "dynamicFilter is null");
            this.staticFilter = requireNonNull(staticFilter, "staticFilter is null");
            this.columnHandles = ImmutableMap.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
            this.converter = requireNonNull(converter, "converter is null");
        }

        public DynamicPageFilterCollector create(PageFunctionCompiler compiler, Optional<String> className)
        {
            return new DynamicPageFilterCollector(domainTranslator, dynamicFilter, staticFilter, columnHandles, converter, compiler, className);
        }
    }
}
