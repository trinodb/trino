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
package io.prestosql.plugin.phoenix;

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.prestosql.plugin.jdbc.JdbcColumnHandle;
import io.prestosql.plugin.jdbc.JdbcIdentity;
import io.prestosql.plugin.jdbc.JdbcTableHandle;
import io.prestosql.plugin.jdbc.QueryBuilder;
import io.prestosql.spi.HostAddress;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplit;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.DynamicFilter;
import io.prestosql.spi.connector.FixedSplitSource;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.phoenix.compile.QueryPlan;
import org.apache.phoenix.jdbc.PhoenixConnection;
import org.apache.phoenix.jdbc.PhoenixPreparedStatement;
import org.apache.phoenix.mapreduce.PhoenixInputSplit;
import org.apache.phoenix.query.KeyRange;

import javax.inject.Inject;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_INTERNAL_ERROR;
import static io.prestosql.plugin.phoenix.PhoenixErrorCode.PHOENIX_SPLIT_ERROR;
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
        this.phoenixClient = requireNonNull(phoenixClient, "client is null");
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
        try (PhoenixConnection connection = phoenixClient.getConnection(JdbcIdentity.from(session))) {
            List<JdbcColumnHandle> columns = tableHandle.getColumns()
                    .map(columnSet -> columnSet.stream().map(JdbcColumnHandle.class::cast).collect(toList()))
                    .orElseGet(() -> phoenixClient.getColumns(session, tableHandle));
            PhoenixPreparedStatement inputQuery = (PhoenixPreparedStatement) new QueryBuilder(phoenixClient).buildSql(
                    session,
                    connection,
                    tableHandle.getRemoteTableName(),
                    tableHandle.getGroupingSets(),
                    columns,
                    tableHandle.getConstraint(),
                    Optional.empty(),
                    Function.identity());

            List<ConnectorSplit> splits = getSplits(inputQuery).stream()
                    .map(PhoenixInputSplit.class::cast)
                    .map(split -> new PhoenixSplit(
                            getSplitAddresses(split),
                            new WrappedPhoenixInputSplit(split),
                            tableHandle.getConstraint()))
                    .collect(toImmutableList());
            return new FixedSplitSource(splits);
        }
        catch (IOException | SQLException e) {
            throw new PrestoException(PHOENIX_SPLIT_ERROR, "Couldn't get Phoenix splits", e);
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
            throw new PrestoException(PHOENIX_INTERNAL_ERROR, "Exception when getting split addresses", e);
        }
    }

    private List<InputSplit> getSplits(PhoenixPreparedStatement inputQuery)
            throws IOException
    {
        QueryPlan queryPlan = phoenixClient.getQueryPlan(inputQuery);
        return generateSplits(queryPlan, queryPlan.getSplits());
    }

    // mostly copied from PhoenixInputFormat, but without the region size calculations
    private List<InputSplit> generateSplits(QueryPlan queryPlan, List<KeyRange> splits)
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
                inputSplits.add(new PhoenixInputSplit(scans, regionSize, regionLocation));
            }
            return inputSplits;
        }
    }
}
