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
package io.trino.operator.project;

import com.google.common.annotations.VisibleForTesting;
import io.trino.annotation.NotThreadSafe;
import io.trino.memory.context.LocalMemoryContext;
import io.trino.operator.WorkProcessor;
import io.trino.spi.Page;
import io.trino.spi.block.DictionaryBlock;
import io.trino.spi.block.DictionaryId;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.sql.gen.columnar.FilterEvaluator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.operator.project.SelectedPositions.positionsRange;
import static io.trino.spi.block.DictionaryId.randomDictionaryId;
import static java.util.Objects.requireNonNull;

@NotThreadSafe
public class PageProcessor
{
    public static final int MAX_BATCH_SIZE = 8 * 1024;
    static final int MAX_PAGE_SIZE_IN_BYTES = 16 * 1024 * 1024;
    static final int MIN_PAGE_SIZE_IN_BYTES = 4 * 1024 * 1024;

    private final DictionarySourceIdFunction dictionarySourceIdFunction = new DictionarySourceIdFunction();
    private final Optional<FilterEvaluator> filterEvaluator;
    private final Optional<FilterEvaluator> dynamicFilterEvaluator;
    private final PageProjectionsProcessor projectionsProcessor;

    public PageProcessor(Optional<FilterEvaluator> filterEvaluator, Optional<FilterEvaluator> dynamicFilterEvaluator, List<? extends PageProjection> projections, OptionalInt initialBatchSize)
    {
        this.filterEvaluator = requireNonNull(filterEvaluator, "filterEvaluator is null");
        this.dynamicFilterEvaluator = requireNonNull(dynamicFilterEvaluator, "dynamicFilterEvaluator is null");
        boolean identityProjections = !projections.isEmpty() && projections.stream().allMatch(InputPageProjection.class::isInstance);
        List<PageProjection> dictionaryAwareProjections = projections.stream()
                .map(projection -> {
                    if (projection.getInputChannels().size() == 1 && projection.isDeterministic()) {
                        return (PageProjection) new DictionaryAwarePageProjection(projection, dictionarySourceIdFunction);
                    }
                    return (PageProjection) projection;
                })
                .collect(toImmutableList());
        int initialProjectBatchSize;
        if (identityProjections) {
            initialProjectBatchSize = MAX_BATCH_SIZE;
        }
        else {
            initialProjectBatchSize = initialBatchSize.orElse(1);
        }
        this.projectionsProcessor = new PageProjectionsProcessor(dictionaryAwareProjections, identityProjections, initialProjectBatchSize);
    }

    @VisibleForTesting
    public PageProcessor(Optional<FilterEvaluator> filterEvaluator, List<? extends PageProjection> projections)
    {
        this(filterEvaluator, Optional.empty(), projections, OptionalInt.of(1));
    }

    @VisibleForTesting
    public Iterator<Optional<Page>> process(ConnectorSession session, LocalMemoryContext memoryContext, SourcePage page)
    {
        WorkProcessor<Page> processor = createWorkProcessor(session, memoryContext, new PageProcessorMetrics(), page);
        return processor.yieldingIterator();
    }

    public WorkProcessor<Page> createWorkProcessor(
            ConnectorSession session,
            LocalMemoryContext memoryContext,
            PageProcessorMetrics metrics,
            SourcePage page)
    {
        // limit the scope of the dictionary ids to just one page
        dictionarySourceIdFunction.reset();

        if (page.getPositionCount() == 0) {
            return WorkProcessor.of();
        }

        SelectedPositions selectedPositions = positionsRange(0, page.getPositionCount());
        if (dynamicFilterEvaluator.isPresent()) {
            FilterEvaluator.SelectionResult dynamicFilterResult = dynamicFilterEvaluator.get().evaluate(session, selectedPositions, page);
            selectedPositions = dynamicFilterResult.selectedPositions();
            metrics.recordDynamicFilterMetrics(dynamicFilterResult.filterTimeNanos(), selectedPositions.size());
        }

        if (filterEvaluator.isPresent()) {
            FilterEvaluator.SelectionResult filterResult = filterEvaluator.get().evaluate(session, selectedPositions, page);
            selectedPositions = filterResult.selectedPositions();
            metrics.recordFilterTime(filterResult.filterTimeNanos());
        }

        if (selectedPositions.isEmpty()) {
            return WorkProcessor.of();
        }

        if (projectionsProcessor.getProjectionCount() == 0) {
            // retained memory for empty page is negligible
            return WorkProcessor.of(new Page(selectedPositions.size()));
        }

        return projectionsProcessor.project(session, memoryContext, metrics, page, selectedPositions);
    }

    @VisibleForTesting
    public List<PageProjection> getProjections()
    {
        return projectionsProcessor.getProjections();
    }

    @NotThreadSafe
    private static class DictionarySourceIdFunction
            implements Function<DictionaryBlock, DictionaryId>
    {
        private final Map<DictionaryId, DictionaryId> dictionarySourceIds = new HashMap<>();

        @Override
        public DictionaryId apply(DictionaryBlock block)
        {
            return dictionarySourceIds.computeIfAbsent(block.getDictionarySourceId(), _ -> randomDictionaryId());
        }

        public void reset()
        {
            dictionarySourceIds.clear();
        }
    }
}
