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

import io.trino.operator.project.DictionaryAwarePageFilter;
import io.trino.operator.project.InputChannels;
import io.trino.operator.project.PageFilter;
import io.trino.operator.project.SelectedPositions;
import io.trino.spi.Page;
import io.trino.spi.connector.ConnectorSession;

import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.operator.project.SelectedPositions.positionsRange;
import static java.util.Objects.requireNonNull;

public class DynamicPageFilter
        implements PageFilter
{
    private Optional<PageFilter> currentFilter;
    private Optional<Supplier<PageFilter>> currentPageFilterSupplier;
    private final DynamicPageFilterCollector collector;

    public DynamicPageFilter(Optional<PageFilter> filter, DynamicPageFilterCollector collector)
    {
        requireNonNull(filter, "filter is null");
        this.collector = requireNonNull(collector, "collector is null");
        this.currentPageFilterSupplier = collector.getFilterFunctionSupplier()
                .or(() -> filter.map(pageFilter -> {
                    if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
                        return () -> new DictionaryAwarePageFilter(pageFilter);
                    }
                    else {
                        return () -> pageFilter;
                    }
                }));
        this.currentFilter = currentPageFilterSupplier.map(Supplier::get);
    }

    @Override
    public boolean isDeterministic()
    {
        return currentFilter.map(PageFilter::isDeterministic).orElse(true);
    }

    @Override
    public InputChannels getInputChannels()
    {
        // DynamicPageFilterCollector can introduce updated PageFilter between getInputChannels and filter calls in PageProcessor
        // To avoid ArrayIndexOutOfBoundsException from PageProcessor.process we update current filter here
        Optional<Supplier<PageFilter>> filterFunctionSupplier = collector.getFilterFunctionSupplier();
        if (!filterFunctionSupplier.equals(currentPageFilterSupplier)) {
            filterFunctionSupplier.ifPresent(this::setUpdatedFilter);
            currentPageFilterSupplier = filterFunctionSupplier;
        }
        return currentFilter.map(PageFilter::getInputChannels).orElse(new InputChannels());
    }

    @Override
    public SelectedPositions filter(ConnectorSession session, Page page)
    {
        return currentFilter.map(pageFilter -> pageFilter.filter(session, page))
                .orElse(positionsRange(0, page.getPositionCount()));
    }

    private void setUpdatedFilter(Supplier<PageFilter> filterFunctionSupplier)
    {
        PageFilter pageFilter = filterFunctionSupplier.get();
        if (pageFilter.getInputChannels().size() == 1 && pageFilter.isDeterministic()) {
            currentFilter = Optional.of(new DictionaryAwarePageFilter(pageFilter));
        }
        else {
            currentFilter = Optional.of(pageFilter);
        }
    }
}
