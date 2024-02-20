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
package io.trino.plugin.pinot.client;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.trino.plugin.pinot.PinotConfig;
import io.trino.plugin.pinot.PinotErrorCode;
import io.trino.plugin.pinot.PinotException;
import io.trino.plugin.pinot.PinotSessionProperties;
import io.trino.plugin.pinot.PinotSplit;
import io.trino.spi.connector.ConnectorSession;
import org.apache.pinot.common.datatable.DataTable;
import org.apache.pinot.common.metrics.BrokerMetrics;
import org.apache.pinot.common.request.BrokerRequest;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.core.transport.AsyncQueryResponse;
import org.apache.pinot.core.transport.QueryRouter;
import org.apache.pinot.core.transport.ServerInstance;
import org.apache.pinot.core.transport.ServerResponse;
import org.apache.pinot.core.transport.ServerRoutingInstance;
import org.apache.pinot.core.transport.server.routing.stats.ServerRoutingStatsManager;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.metrics.PinotMetricUtils;
import org.apache.pinot.spi.metrics.PinotMetricsRegistry;
import org.apache.pinot.spi.utils.builder.TableNameBuilder;
import org.apache.pinot.sql.parsers.CalciteSqlCompiler;
import org.apache.pinot.sql.parsers.SqlCompilationException;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.IntStream;

import static io.trino.plugin.pinot.PinotErrorCode.PINOT_EXCEPTION;
import static io.trino.plugin.pinot.PinotErrorCode.PINOT_INVALID_PQL_GENERATED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotLegacyDataFetcher
        implements PinotDataFetcher
{
    private static final Logger LOG = Logger.get(PinotLegacyDataFetcher.class);

    private final ConnectorSession session;
    private final PinotLegacyServerQueryClient pinotQueryClient;
    private final PinotSplit split;

    private final String query;
    private final LinkedList<PinotDataTableWithSize> dataTableList = new LinkedList<>();
    private final RowCountChecker rowCountChecker;
    private long readTimeNanos;
    private long estimatedMemoryUsageInBytes;
    private boolean isPinotDataFetched;

    public PinotLegacyDataFetcher(ConnectorSession session, PinotLegacyServerQueryClient pinotQueryClient, PinotSplit split, String query, RowCountChecker rowCountChecker)
    {
        this.session = requireNonNull(session, "session is null");
        this.pinotQueryClient = requireNonNull(pinotQueryClient, "pinotQueryClient is null");
        this.split = requireNonNull(split, "split is null");
        this.query = requireNonNull(query, "query is null");
        this.rowCountChecker = requireNonNull(rowCountChecker, "rowCountChecker is null");
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public long getMemoryUsageBytes()
    {
        return estimatedMemoryUsageInBytes;
    }

    @Override
    public boolean endOfData()
    {
        return dataTableList.isEmpty();
    }

    @Override
    public boolean isDataFetched()
    {
        return isPinotDataFetched;
    }

    @Override
    public void fetchData()
    {
        long startTimeNanos = System.nanoTime();
        try {
            queryPinot().forEachRemaining(dataTableWithSize -> {
                checkExceptions(dataTableWithSize.getDataTable(), split, query);
                rowCountChecker.checkTooManyRows(dataTableWithSize.getDataTable());
                dataTableList.add(dataTableWithSize);
                estimatedMemoryUsageInBytes += dataTableWithSize.getEstimatedSizeInBytes();
            });

            isPinotDataFetched = true;
        }
        finally {
            readTimeNanos += System.nanoTime() - startTimeNanos;
        }
    }

    @Override
    public PinotDataTableWithSize getNextDataTable()
    {
        PinotDataTableWithSize dataTableWithSize = dataTableList.pop();
        estimatedMemoryUsageInBytes -= dataTableWithSize.getEstimatedSizeInBytes();
        return dataTableWithSize;
    }

    private Iterator<PinotDataTableWithSize> queryPinot()
    {
        String host = split.getSegmentHost().orElseThrow(() -> new PinotException(PinotErrorCode.PINOT_INVALID_PQL_GENERATED, Optional.empty(), "Expected the segment split to contain the host"));
        LOG.debug("Query '%s' on host '%s' for segment splits: %s", query, split.getSegmentHost(), split.getSegments());
        return pinotQueryClient.queryPinot(
                session,
                query,
                host,
                split.getSegments());
    }

    public static class Factory
            implements PinotDataFetcher.Factory
    {
        private final PinotLegacyServerQueryClient queryClient;
        private final int limitForSegmentQueries;

        @Inject
        public Factory(PinotHostMapper pinotHostMapper, PinotConfig pinotConfig, PinotLegacyServerQueryClientConfig pinotLegacyServerQueryClientConfig)
        {
            requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            this.limitForSegmentQueries = pinotLegacyServerQueryClientConfig.getMaxRowsPerSplitForSegmentQueries();
            this.queryClient = new PinotLegacyServerQueryClient(pinotHostMapper, pinotConfig);
        }

        @Override
        public PinotDataFetcher create(ConnectorSession session, String query, PinotSplit split)
        {
            return new PinotLegacyDataFetcher(session, queryClient, split, query, new RowCountChecker(limitForSegmentQueries, query));
        }

        @Override
        public int getRowLimit()
        {
            return limitForSegmentQueries;
        }
    }

    public static class PinotLegacyServerQueryClient
    {
        private static final String TRINO_HOST_PREFIX = "trino-pinot-master";

        private final String trinoHostId;
        private final BrokerMetrics brokerMetrics;
        private final QueryRouter queryRouter;
        private final PinotHostMapper pinotHostMapper;
        private final AtomicLong requestIdGenerator = new AtomicLong();
        private final int estimatedNonNumericColumnSize;

        public PinotLegacyServerQueryClient(PinotHostMapper pinotHostMapper, PinotConfig pinotConfig)
        {
            trinoHostId = getDefaultTrinoId();
            this.pinotHostMapper = requireNonNull(pinotHostMapper, "pinotHostMapper is null");
            this.estimatedNonNumericColumnSize = pinotConfig.getEstimatedSizeInBytesForNonNumericColumn();
            PinotMetricsRegistry registry = PinotMetricUtils.getPinotMetricsRegistry();
            this.brokerMetrics = new BrokerMetrics(registry);
            brokerMetrics.initializeGlobalMeters();
            queryRouter = new QueryRouter(trinoHostId, brokerMetrics, new ServerRoutingStatsManager(new PinotConfiguration()));
        }

        private static String getDefaultTrinoId()
        {
            String defaultBrokerId;
            try {
                defaultBrokerId = TRINO_HOST_PREFIX + InetAddress.getLocalHost().getHostName();
            }
            catch (UnknownHostException e) {
                defaultBrokerId = TRINO_HOST_PREFIX;
            }
            return defaultBrokerId;
        }

        public Iterator<PinotDataTableWithSize> queryPinot(ConnectorSession session, String query, String serverHost, List<String> segments)
        {
            long connectionTimeoutInMillis = PinotSessionProperties.getConnectionTimeout(session).toMillis();
            int pinotRetryCount = PinotSessionProperties.getPinotRetryCount(session);
            // TODO: separate into offline and realtime methods
            BrokerRequest brokerRequest;
            try {
                brokerRequest = CalciteSqlCompiler.compileToBrokerRequest(query);
            }
            catch (SqlCompilationException e) {
                throw new PinotException(PINOT_INVALID_PQL_GENERATED, Optional.of(query), format("Parsing error with on %s, Error = %s", serverHost, e.getMessage()), e);
            }
            ServerInstance serverInstance = pinotHostMapper.getServerInstance(serverHost);
            Map<ServerInstance, List<String>> routingTable = new HashMap<>();
            routingTable.put(serverInstance, new ArrayList<>(segments));
            String tableName = brokerRequest.getQuerySource().getTableName();
            String rawTableName = TableNameBuilder.extractRawTableName(tableName);
            Map<ServerInstance, List<String>> offlineRoutingTable = TableNameBuilder.isOfflineTableResource(tableName) ? routingTable : null;
            Map<ServerInstance, List<String>> realtimeRoutingTable = TableNameBuilder.isRealtimeTableResource(tableName) ? routingTable : null;
            BrokerRequest offlineBrokerRequest = TableNameBuilder.isOfflineTableResource(tableName) ? brokerRequest : null;
            BrokerRequest realtimeBrokerRequest = TableNameBuilder.isRealtimeTableResource(tableName) ? brokerRequest : null;
            AsyncQueryResponse asyncQueryResponse =
                    doWithRetries(pinotRetryCount, requestId -> queryRouter.submitQuery(requestId, rawTableName, offlineBrokerRequest, offlineRoutingTable, realtimeBrokerRequest, realtimeRoutingTable, connectionTimeoutInMillis));
            try {
                Map<ServerRoutingInstance, ServerResponse> response = asyncQueryResponse.getFinalResponses();
                ImmutableList.Builder<PinotDataTableWithSize> pinotDataTableWithSizeBuilder = ImmutableList.builder();
                for (Map.Entry<ServerRoutingInstance, ServerResponse> entry : response.entrySet()) {
                    ServerResponse serverResponse = entry.getValue();
                    DataTable dataTable = serverResponse.getDataTable();
                    // ignore empty tables and tables with 0 rows
                    if (dataTable != null && dataTable.getNumberOfRows() > 0) {
                        // Store each dataTable which will later be constructed into Pages.
                        // Also update estimatedMemoryUsage, mostly represented by the size of all dataTables, using numberOfRows and fieldTypes combined as an estimate
                        long estimatedTableSizeInBytes = IntStream.rangeClosed(0, dataTable.getDataSchema().size() - 1)
                                .mapToLong(i -> getEstimatedColumnSizeInBytes(dataTable.getDataSchema().getColumnDataType(i)) * dataTable.getNumberOfRows())
                                .reduce(0, Long::sum);
                        pinotDataTableWithSizeBuilder.add(new PinotDataTableWithSize(dataTable, estimatedTableSizeInBytes));
                    }
                }
                return pinotDataTableWithSizeBuilder.build().iterator();
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PinotException(PINOT_EXCEPTION, Optional.of(query), "Pinot query execution was interrupted", e);
            }
        }

        private <T> T doWithRetries(int retries, Function<Long, T> caller)
        {
            PinotException firstError = null;
            for (int i = 0; i < retries; ++i) {
                try {
                    return caller.apply(requestIdGenerator.getAndIncrement());
                }
                catch (PinotException e) {
                    if (firstError == null) {
                        firstError = e;
                    }
                    if (!e.isRetryable()) {
                        throw e;
                    }
                }
            }
            throw firstError;
        }

        /**
         * Get estimated size in bytes for the Pinot column.
         * Deterministic for numeric fields; use estimate for other types to save calculation.
         *
         * @param dataType FieldSpec.dataType for Pinot column.
         * @return estimated size in bytes.
         */
        private long getEstimatedColumnSizeInBytes(DataSchema.ColumnDataType dataType)
        {
            if (dataType.isNumber()) {
                switch (dataType) {
                    case LONG:
                        return Long.BYTES;
                    case FLOAT:
                        return Float.BYTES;
                    case DOUBLE:
                        return Double.BYTES;
                    case INT:
                    default:
                        return Integer.BYTES;
                }
            }
            return estimatedNonNumericColumnSize;
        }
    }
}
