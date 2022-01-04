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
package io.trino.plugin.phoenix5;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.log.Logger;
import io.trino.plugin.jdbc.JdbcColumnHandle;
import io.trino.plugin.jdbc.JdbcTableHandle;
import io.trino.spi.HostAddress;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitManager;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.ConnectorTransactionHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.connector.FixedSplitSource;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.query.KeyRange;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_INTERNAL_ERROR;
import static io.trino.plugin.phoenix5.PhoenixErrorCode.PHOENIX_SPLIT_ERROR;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.phoenix.coprocessor.BaseScannerRegionObserver.EXPECTED_UPPER_REGION_KEY;

public class PhoenixSplitManager
        implements ConnectorSplitManager
{
    private static final Logger log = Logger.get(PhoenixSplitManager.class);

    private final PhoenixClient phoenixClient;

    @Inject
    public PhoenixSplitManager(PhoenixClient phoenixClient)
    {
        this.phoenixClient = requireNonNull(phoenixClient, "phoenixClient is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle transaction,
            ConnectorSession session,
            ConnectorTableHandle table,
            SplitSchedulingStrategy splitSchedulingStrategy,
            DynamicFilter dynamicFilter)
    {
        JdbcTableHandle tableHandle = (JdbcTableHandle) table;
        try (Connection connection = phoenixClient.getConnection(session)) {
            List<JdbcColumnHandle> columns = tableHandle.getColumns()
                    .map(columnSet -> columnSet.stream().map(JdbcColumnHandle.class::cast).collect(toList()))
                    .orElseGet(() -> phoenixClient.getColumns(session, tableHandle));
            PhoenixPreparedStatement inputQuery = (PhoenixPreparedStatement) phoenixClient.prepareStatement(
                    session,
                    connection,
                    tableHandle,
                    columns,
                    Optional.empty());

            int maxScansPerSplit = session.getProperty(PhoenixSessionProperties.MAX_SCANS_PER_SPLIT, Integer.class);
            List<ConnectorSplit> splits = getSplits(inputQuery, maxScansPerSplit).stream()
                    .map(PhoenixInputSplit.class::cast)
                    .map(split -> new PhoenixSplit(
                            getSplitAddresses(split),
                            SerializedPhoenixInputSplit.serialize(split)))
                    .collect(toImmutableList());
            return new FixedSplitSource(splits);
        }
        catch (IOException | SQLException e) {
            throw new TrinoException(PHOENIX_SPLIT_ERROR, "Couldn't get Phoenix splits", e);
        }
    }

    private List<HostAddress> getSplitAddresses(PhoenixInputSplit split)
    {
        try {
            return ImmutableList.of(HostAddress.fromString(split.getLocations()[0]));
        }
        catch (IOException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new TrinoException(PHOENIX_INTERNAL_ERROR, "Exception when getting split addresses", e);
        }
    }

    private List<InputSplit> getSplits(PhoenixPreparedStatement inputQuery, int maxScansPerSplit)
            throws IOException
    {
        QueryPlan queryPlan = phoenixClient.getQueryPlan(inputQuery);
        return generateSplits(queryPlan, queryPlan.getSplits(), maxScansPerSplit);
    }

    // mostly copied from PhoenixInputFormat, but without the region size calculations
    private List<InputSplit> generateSplits(QueryPlan queryPlan, List<KeyRange> splits, int maxScansPerSplit)
            throws IOException
    {
        requireNonNull(queryPlan, "queryPlan is null");
        requireNonNull(splits, "splits is null");

        try (org.apache.hadoop.hbase.client.Connection connection = phoenixClient.getHConnection()) {
            RegionLocator regionLocator = connection.getRegionLocator(TableName.valueOf(queryPlan.getTableRef().getTable().getPhysicalName().toString()));
            long regionSize = -1;
            List<InputSplit> inputSplits = new ArrayList<>(splits.size());
            for (List<Scan> scans : queryPlan.getScans()) {
                HRegionLocation location = regionLocator.getRegionLocation(scans.get(0).getStartRow(), false);
                String regionLocation = location.getHostname();

                if (log.isDebugEnabled()) {
                    log.debug(
                            "Scan count[%d] : %s ~ %s",
                            scans.size(),
                            Bytes.toStringBinary(scans.get(0).getStartRow()),
                            Bytes.toStringBinary(scans.get(scans.size() - 1).getStopRow()));
                    log.debug("First scan : %swith scanAttribute : %s [scanCache, cacheBlock, scanBatch] : [%d, %s, %d] and  regionLocation : %s",
                            scans.get(0), scans.get(0).getAttributesMap(), scans.get(0).getCaching(), scans.get(0).getCacheBlocks(), scans.get(0).getBatch(), regionLocation);
                    for (int i = 0, limit = scans.size(); i < limit; i++) {
                        log.debug("EXPECTED_UPPER_REGION_KEY[%d] : %s", i, Bytes.toStringBinary(scans.get(i).getAttribute(EXPECTED_UPPER_REGION_KEY)));
                    }
                }
                /*
                 * Handle parallel execution explicitly in Trino rather than internally in Phoenix.
                 * Each split is handled by a single ConcatResultIterator
                 * (See PhoenixClient.getResultSet(...))
                 */
                for (List<Scan> splitScans : Lists.partition(scans, maxScansPerSplit)) {
                    inputSplits.add(new PhoenixInputSplit(splitScans, regionSize, regionLocation));
                }
            }
            return inputSplits;
        }
    }
}
