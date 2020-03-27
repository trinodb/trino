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
package io.prestosql.plugin.memory;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.Page;
import io.prestosql.spi.connector.ColumnHandle;
import io.prestosql.spi.connector.ConnectorPageSource;
import io.prestosql.spi.connector.ConnectorPageSourceProvider;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.FixedPageSource;
import io.prestosql.spi.predicate.Domain;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeUtils;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public final class MemoryPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final MemoryPagesStore pagesStore;

    @Inject
    public MemoryPageSourceProvider(MemoryPagesStore pagesStore)
    {
        this.pagesStore = requireNonNull(pagesStore, "pagesStore is null");
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle table,
            List<ColumnHandle> columns,
            TupleDomain<ColumnHandle> dynamicFilter)
    {
        MemorySplit memorySplit = (MemorySplit) split;
        long tableId = memorySplit.getTable();
        int partNumber = memorySplit.getPartNumber();
        int totalParts = memorySplit.getTotalPartsPerWorker();
        long expectedRows = memorySplit.getExpectedRows();
        MemoryTableHandle memoryTable = (MemoryTableHandle) table;
        OptionalDouble sampleRatio = memoryTable.getSampleRatio();

        if (dynamicFilter.isNone()) {
            return new FixedPageSource(ImmutableList.of());
        }
        Map<Integer, Domain> domains = dynamicFilter
                .transform(columns::indexOf)
                .getDomains()
                .get();

        List<Integer> columnIndexes = columns.stream()
                .map(MemoryColumnHandle.class::cast)
                .map(MemoryColumnHandle::getColumnIndex).collect(toList());
        List<Page> pages = pagesStore.getPages(
                tableId,
                partNumber,
                totalParts,
                columnIndexes,
                expectedRows,
                memorySplit.getLimit(),
                sampleRatio);
        return new FixedPageSource(pages.stream()
                .map(page -> applyFilter(page, domains))
                .collect(toList()));
    }

    private Page applyFilter(Page page, Map<Integer, Domain> domains)
    {
        int[] positions = new int[page.getPositionCount()];
        int length = 0;
        for (int position = 0; position < page.getPositionCount(); ++position) {
            if (positionMatchesPredicate(page, position, domains)) {
                positions[length++] = position;
            }
        }
        return page.getPositions(positions, 0, length);
    }

    private boolean positionMatchesPredicate(Page page, int position, Map<Integer, Domain> domains)
    {
        for (Map.Entry<Integer, Domain> entry : domains.entrySet()) {
            int channel = entry.getKey();
            Domain domain = entry.getValue();
            Object value = TypeUtils.readNativeValue(domain.getType(), page.getBlock(channel), position);
            if (!domain.includesNullableValue(value)) {
                return false;
            }
        }

        return true;
    }
}
