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
package io.trino.plugin.paimon;

import com.google.inject.Inject;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;

import java.util.List;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Trino {@link ConnectorSplitManager}.
 */
public class PaimonSplitManager
        implements ConnectorSplitManager
{
    private final PaimonTransactionManager transactionManager;

    @Inject
    public PaimonSplitManager(PaimonTransactionManager transactionManager)
    {
        this.transactionManager = requireNonNull(transactionManager, "transactionManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle handle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        // TODO dynamicFilter?
        // TODO what is constraint?
        PaimonMetadata metadata = transactionManager.get(transaction, session.getIdentity());
        PaimonTableHandle tableHandle = (PaimonTableHandle) handle;
        Table table = tableHandle.tableWithDynamicOptions(metadata.catalog(), session);
        ReadBuilder readBuilder = table.newReadBuilder();
        new PaimonFilterConverter(table.rowType())
                .convert(tableHandle.getPredicate())
                .ifPresent(readBuilder::withFilter);
        tableHandle.getLimit().ifPresent(limit -> readBuilder.withLimit((int) limit));
        List<Split> splits = readBuilder.dropStats().newScan().plan().splits();

        long maxRowCount = splits.stream().mapToLong(Split::rowCount).max().orElse(0L);
        double minimumSplitWeight = PaimonSessionProperties.getMinimumSplitWeight(session);
        return new PaimonSplitSource(
                splits.stream()
                        .map(
                                split ->
                                        PaimonSplit.fromSplit(
                                                split,
                                                Math.min(
                                                        Math.max(
                                                                (double) split.rowCount()
                                                                        / maxRowCount,
                                                                minimumSplitWeight),
                                                        1.0)))
                        .collect(Collectors.toList()),
                tableHandle.getLimit());
    }
}
