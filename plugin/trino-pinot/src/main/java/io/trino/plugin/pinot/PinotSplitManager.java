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

import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import io.trino.plugin.pinot.client.PinotClient;
import io.trino.spi.ErrorCode;
import io.trino.spi.ErrorCodeSupplier;
import io.trino.spi.ErrorType;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.pinot.spi.config.table.TableType;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static io.trino.plugin.pinot.PinotSessionProperties.getNonAggregateLimitForBrokerQueries;
import static io.trino.plugin.pinot.PinotSessionProperties.isPreferBrokerQueries;
import static io.trino.plugin.pinot.PinotSplit.createBrokerSplit;
import static io.trino.plugin.pinot.PinotSplit.createSegmentSplit;
import static io.trino.spi.ErrorType.USER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class PinotSplitManager
        implements ConnectorSplitManager
{
    private static final Logger LOG = Logger.get(PinotSplitManager.class);
    private static final String REALTIME_SUFFIX = "_" + TableType.REALTIME;
    private static final String OFFLINE_SUFFIX = "_" + TableType.OFFLINE;

    private final PinotClient pinotClient;

    @Inject
    public PinotSplitManager(PinotClient pinotClient)
    {
        this.pinotClient = requireNonNull(pinotClient, "pinotClient is null");
    }

    protected ConnectorSplitSource generateSplitForBrokerBasedScan(PinotTableHandle pinotTableHandle)
    {
        return new FixedSplitSource(createBrokerSplit());
    }

    protected ConnectorSplitSource generateSplitsForSegmentBasedScan(
            PinotTableHandle tableHandle,
            ConnectorSession session)
    {
        String tableName = tableHandle.getTableName();
        Map<String, Map<String, List<String>>> routingTable = pinotClient.getRoutingTableForTable(tableName);
        LOG.info("Got routing table for %s: %s", tableName, routingTable);
        List<ConnectorSplit> splits = new ArrayList<>();
        if (!routingTable.isEmpty()) {
            PinotClient.TimeBoundary timeBoundary = new PinotClient.TimeBoundary(null, null);
            if (routingTable.containsKey(tableName + REALTIME_SUFFIX) && routingTable.containsKey(tableName + OFFLINE_SUFFIX)) {
                timeBoundary = pinotClient.getTimeBoundaryForTable(tableName);
            }
            generateSegmentSplits(splits, routingTable, tableName, REALTIME_SUFFIX, session, timeBoundary.getOnlineTimePredicate());
            generateSegmentSplits(splits, routingTable, tableName, OFFLINE_SUFFIX, session, timeBoundary.getOfflineTimePredicate());
        }

        Collections.shuffle(splits);
        return new FixedSplitSource(splits);
    }

    protected void generateSegmentSplits(
            List<ConnectorSplit> splits,
            Map<String, Map<String, List<String>>> routingTable,
            String tableName,
            String tableNameSuffix,
            ConnectorSession session,
            Optional<String> timePredicate)
    {
        String finalTableName = tableName + tableNameSuffix;
        int segmentsPerSplitConfigured = PinotSessionProperties.getSegmentsPerSplit(session);
        for (String routingTableName : routingTable.keySet()) {
            if (!routingTableName.equalsIgnoreCase(finalTableName)) {
                continue;
            }

            Map<String, List<String>> hostToSegmentsMap = routingTable.get(routingTableName);
            hostToSegmentsMap.forEach((host, segments) -> {
                int numSegmentsInThisSplit = Math.min(segments.size(), segmentsPerSplitConfigured);
                // segments is already shuffled
                Iterables.partition(segments, numSegmentsInThisSplit).forEach(
                        segmentsForThisSplit -> splits.add(
                                createSegmentSplit(tableNameSuffix, segmentsForThisSplit, host, timePredicate)));
            });
        }
    }

    public enum QueryNotAdequatelyPushedDownErrorCode
            implements ErrorCodeSupplier
    {
        PQL_NOT_PRESENT(1, USER_ERROR, "Query uses unsupported expressions that cannot be pushed into the storage engine.");

        private final ErrorCode errorCode;

        QueryNotAdequatelyPushedDownErrorCode(int code, ErrorType type, String guidance)
        {
            errorCode = new ErrorCode(code + 0x0625_0000, name() + ": " + guidance, type);
        }

        @Override
        public ErrorCode toErrorCode()
        {
            return errorCode;
        }
    }

    public static class QueryNotAdequatelyPushedDownException
            extends TrinoException
    {
        private final String connectorId;
        private final ConnectorTableHandle connectorTableHandle;

        public QueryNotAdequatelyPushedDownException(
                QueryNotAdequatelyPushedDownErrorCode errorCode,
                ConnectorTableHandle connectorTableHandle,
                String connectorId)
        {
            super(requireNonNull(errorCode, "errorCode is null"), (String) null);
            this.connectorId = requireNonNull(connectorId, "connectorId is null");
            this.connectorTableHandle = requireNonNull(connectorTableHandle, "connectorTableHandle is null");
        }

        @Override
        public String getMessage()
        {
            return super.getMessage() + format(" table: %s:%s", connectorId, connectorTableHandle);
        }
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableHandle tableHandle,
            DynamicFilter dynamicFilter,
            Constraint constraint)
    {
        PinotTableHandle pinotTableHandle = (PinotTableHandle) tableHandle;
        Supplier<TrinoException> errorSupplier = () -> new QueryNotAdequatelyPushedDownException(QueryNotAdequatelyPushedDownErrorCode.PQL_NOT_PRESENT, pinotTableHandle, "");
        if (!isBrokerQuery(session, pinotTableHandle)) {
            if (PinotSessionProperties.isForbidSegmentQueries(session)) {
                throw errorSupplier.get();
            }
            return generateSplitsForSegmentBasedScan(pinotTableHandle, session);
        }
        return generateSplitForBrokerBasedScan(pinotTableHandle);
    }

    private static boolean isBrokerQuery(ConnectorSession session, PinotTableHandle tableHandle)
    {
        return tableHandle.getQuery().isPresent() ||
                (isPreferBrokerQueries(session) && tableHandle.getLimit().orElse(Integer.MAX_VALUE) < getNonAggregateLimitForBrokerQueries(session));
    }
}
