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
package io.trino.plugin.pinot;

import com.google.inject.Inject;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.plugin.pinot.client.PinotDataFetcher;
import io.trino.plugin.pinot.query.DynamicTable;
import io.trino.plugin.pinot.query.PinotQueryInfo;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorPageSourceProvider;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;

import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.pinot.query.DynamicTablePqlExtractor.extractPql;
import static io.trino.plugin.pinot.query.PinotQueryBuilder.generatePql;
import static java.util.Objects.requireNonNull;

public class PinotPageSourceProvider
        implements ConnectorPageSourceProvider
{
    private final PinotClient clusterInfoFetcher;
    private final int limitForSegmentQueries;
    private final int limitForBrokerQueries;
    private final long targetSegmentPageSizeBytes;
    private final PinotDataFetcher.Factory pinotDataFetcherFactory;

    @Inject
    public PinotPageSourceProvider(
            PinotConfig pinotConfig,
            PinotClient clusterInfoFetcher,
            PinotDataFetcher.Factory pinotDataFetcherFactory)
    {
        this.clusterInfoFetcher = requireNonNull(clusterInfoFetcher, "clusterInfoFetcher is null");
        this.pinotDataFetcherFactory = requireNonNull(pinotDataFetcherFactory, "pinotDataFetcherFactory is null");
        this.limitForSegmentQueries = pinotDataFetcherFactory.getRowLimit();
        this.limitForBrokerQueries = pinotConfig.getMaxRowsForBrokerQueries();
        this.targetSegmentPageSizeBytes = pinotConfig.getTargetSegmentPageSize().toBytes();
    }

    @Override
    public ConnectorPageSource createPageSource(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorSplit split,
            ConnectorTableHandle tableHandle,
            List<ColumnHandle> columns,
            DynamicFilter dynamicFilter)
    {
        requireNonNull(split, "split is null");

        PinotSplit pinotSplit = (PinotSplit) split;

        List<PinotColumnHandle> handles = new ArrayList<>();
        for (ColumnHandle handle : columns) {
            handles.add((PinotColumnHandle) handle);
        }
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        String query = generatePql(pinotTableHandle, handles, pinotSplit.getSuffix(), pinotSplit.getTimePredicate(), limitForSegmentQueries);

        switch (pinotSplit.getSplitType()) {
            case SEGMENT:
                PinotDataFetcher pinotDataFetcher = pinotDataFetcherFactory.create(session, query, pinotSplit);
                return new PinotSegmentPageSource(
                        targetSegmentPageSizeBytes,
                        handles,
                        pinotDataFetcher);
            case BROKER:
                PinotQueryInfo pinotQueryInfo;
                if (pinotTableHandle.getQuery().isPresent()) {
                    DynamicTable dynamicTable = pinotTableHandle.getQuery().get();
                    pinotQueryInfo = new PinotQueryInfo(dynamicTable.getTableName(),
                            extractPql(dynamicTable, pinotTableHandle.getConstraint()),
                            dynamicTable.getGroupingColumns().size());
                }
                else {
                    pinotQueryInfo = new PinotQueryInfo(pinotTableHandle.getTableName(), query, 0);
                }

                return new PinotBrokerPageSource(
                        session,
                        pinotQueryInfo,
                        handles,
                        clusterInfoFetcher,
                        limitForBrokerQueries);
        }
        throw new UnsupportedOperationException("Unknown Pinot split type: " + pinotSplit.getSplitType());
    }
}
