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
package io.prestosql.iceberg;

import com.netflix.iceberg.Table;
import com.netflix.iceberg.TableScan;
import io.prestosql.plugin.hive.HdfsEnvironment;
import io.prestosql.plugin.hive.HiveColumnHandle;
import io.prestosql.plugin.hive.TypeTranslator;
import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.connector.ConnectorSplitManager;
import io.prestosql.spi.connector.ConnectorSplitSource;
import io.prestosql.spi.connector.ConnectorTableLayoutHandle;
import io.prestosql.spi.connector.ConnectorTransactionHandle;
import io.prestosql.spi.connector.classloader.ClassLoaderSafeConnectorSplitSource;
import io.prestosql.spi.predicate.TupleDomain;
import io.prestosql.spi.type.TypeManager;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.Map;
import java.util.stream.Collectors;

import static io.prestosql.iceberg.IcebergUtil.SNAPSHOT_ID;
import static io.prestosql.iceberg.IcebergUtil.SNAPSHOT_TIMESTAMP_MS;

public class IcebergSplitManager
        implements ConnectorSplitManager
{
    private final HdfsEnvironment hdfsEnvironment;
    private final TypeTranslator typeTranslator;
    private final TypeManager typeRegistry;
    private final IcebergUtil icebergUtil;

    @Inject
    public IcebergSplitManager(
            HdfsEnvironment hdfsEnvironment,
            TypeTranslator typeTranslator,
            TypeManager typeRegistry,
            IcebergUtil icebergUtil)
    {
        this.hdfsEnvironment = hdfsEnvironment;
        this.typeTranslator = typeTranslator;
        this.typeRegistry = typeRegistry;
        this.icebergUtil = icebergUtil;
    }

    @Override
    public ConnectorSplitSource getSplits(ConnectorTransactionHandle transactionHandle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingStrategy splitSchedulingStrategy)
    {
        IcebergTableLayoutHandle tbl = (IcebergTableLayoutHandle) layout;
        TupleDomain<HiveColumnHandle> predicates = tbl.getPredicates().getDomains()
                .map(m -> m.entrySet().stream().collect(Collectors.toMap((x) -> HiveColumnHandle.class.cast(x.getKey()), Map.Entry::getValue)))
                .map(m -> TupleDomain.withColumnDomains(m)).orElse(TupleDomain.none());

        Configuration configuration = hdfsEnvironment.getConfiguration(new HdfsEnvironment.HdfsContext(session, tbl.getDatabase()), new Path("file:///tmp"));
        Table icebergTable = icebergUtil.getIcebergTable(tbl.getDatabase(), tbl.getTableName(), configuration);
        Long snapshotId = icebergUtil.getPredicateValue(predicates, SNAPSHOT_ID);
        Long snapshotTimestamp = icebergUtil.getPredicateValue(predicates, SNAPSHOT_TIMESTAMP_MS);
        TableScan tableScan = icebergUtil.getTableScan(session, predicates, snapshotId, snapshotTimestamp, icebergTable);

        // We set these values to current snapshotId to ensure if user projects these columns they get the actual values and not null when these columns are not specified
        // in predicates.
        Long currentSnapshotId = icebergTable.currentSnapshot() != null ? icebergTable.currentSnapshot().snapshotId() : null;
        Long currentSnapshotTimestamp = icebergTable.currentSnapshot() != null ? icebergTable.currentSnapshot().timestampMillis() : null;

        snapshotId = snapshotId != null ? snapshotId : currentSnapshotId;
        snapshotTimestamp = snapshotTimestamp != null ? snapshotTimestamp : currentSnapshotTimestamp;
        // TODO Use residual. Right now there is no way to propagate residual to presto but at least we can
        // propagate it at split level so the parquet pushdown can leverage it.
        final IcebergSplitSource icebergSplitSource = new IcebergSplitSource(
                tbl.getDatabase(),
                tbl.getTableName(),
                tableScan.planTasks().iterator(),
                predicates,
                session,
                icebergTable.schema(),
                hdfsEnvironment,
                typeTranslator,
                typeRegistry,
                tbl.getNameToColumnHandle(),
                snapshotId,
                snapshotTimestamp);
        return new ClassLoaderSafeConnectorSplitSource(icebergSplitSource, Thread.currentThread().getContextClassLoader());
    }
}
