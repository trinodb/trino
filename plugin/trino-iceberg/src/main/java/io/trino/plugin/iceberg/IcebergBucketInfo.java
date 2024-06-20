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
package io.trino.plugin.iceberg;

import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.predicate.TupleDomain;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;

import java.util.List;
import java.util.Optional;

import static io.trino.plugin.iceberg.ExpressionConverter.toIcebergExpression;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isAllowMultiSpecBucketExecution;
import static io.trino.plugin.iceberg.PartitionFields.BucketedPartitionField.getBucketedPartitionFields;
import static java.util.Objects.requireNonNull;

class IcebergBucketInfo
{
    private final int specId;
    private final List<PartitionFields.BucketedPartitionField> partitionFields;

    public IcebergBucketInfo(int specId, List<PartitionFields.BucketedPartitionField> partitionFields)
    {
        this.specId = specId;
        this.partitionFields = requireNonNull(partitionFields, "partitionFields is null");
    }

    static Optional<IcebergBucketInfo> getIcebergBucketInfo(
            ConnectorSession session,
            Table icebergTable, IcebergTableHandle table,
            TupleDomain<IcebergColumnHandle> enforcedPredicate)
    {
        if (enforcedPredicate.isNone()) {
            return Optional.empty();
        }
        if (icebergTable.specs().size() == 1) {
            return Optional.of(new IcebergBucketInfo(icebergTable.spec().specId(), getBucketedPartitionFields(icebergTable.spec())));
        }
        if (icebergTable.specs().size() > 1 && enforcedPredicate.isAll()) {
            return Optional.empty();
        }
        if (!isAllowMultiSpecBucketExecution(session)) {
            return Optional.empty();
        }
        TableScan scan = icebergTable.newScan()
                .useSnapshot(table.getSnapshotId().get())
                .filter(toIcebergExpression(enforcedPredicate));
        IcebergBucketInfo icebergBucketInfo = null;
        for (FileScanTask file : scan.planFiles()) {
            if (icebergBucketInfo == null) {
                List<PartitionFields.BucketedPartitionField> fields = getBucketedPartitionFields(file.spec());
                if (fields.isEmpty()) {
                    // currently we only allows storage partitioned join for bucketed tables
                    // we can potentially allow other partition columns if they are used in the bucketed tables and compatible.
                    // however, that wil requires us to efficiently scan all partitions and keep them in memory.
                    return Optional.empty();
                }
                icebergBucketInfo = new IcebergBucketInfo(file.spec().specId(), fields);
            }
            else if (icebergBucketInfo.getSpecId() != file.spec().specId()) {
                // Currently we only allows storage partitioned join for one spec
                // However, we can extend the support to multiple specs if the columns used for join is unchanged in specs.
                return Optional.empty();
            }
        }
        return Optional.ofNullable(icebergBucketInfo);
    }

    public int getSpecId()
    {
        return specId;
    }

    public List<PartitionFields.BucketedPartitionField> getPartitionFields()
    {
        return partitionFields;
    }
}
