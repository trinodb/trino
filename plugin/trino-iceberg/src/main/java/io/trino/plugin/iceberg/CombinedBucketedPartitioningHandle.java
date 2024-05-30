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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import io.trino.spi.connector.ConnectorPartitioningHandle;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static io.trino.plugin.iceberg.IcebergSessionProperties.isOptimizedMismatchedBucketCount;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/***
 * CombinedBucketedPartitioningHandle contains a common partitioning handle for the plan fragment.
 * It contains a table key to table based BucketedIcebergPartitioningHandle so that we can map back
 * to BucketedIcebergPartitioningHandle for given tables.
 */
public class CombinedBucketedPartitioningHandle
        implements BucketedPartitioningHandle
{
    private final Map<TableVersion, BucketedIcebergPartitioningHandle> tablePartitioningHandles;
    private final List<Integer> maxCompatibleBucketCounts;

    @JsonCreator
    public CombinedBucketedPartitioningHandle(
            @JsonProperty("tablePartitioningHandles") Map<TableVersion, BucketedIcebergPartitioningHandle> tablePartitioningHandles,
            @JsonProperty("maxCompatibleBucketCounts") List<Integer> maxCompatibleBucketCounts)
    {
        this.tablePartitioningHandles = requireNonNull(tablePartitioningHandles, "tablePartitioningHandles is null");
        this.maxCompatibleBucketCounts = requireNonNull(maxCompatibleBucketCounts, "maxCompatibleBucketCounts is null");
    }

    public static CombinedBucketedPartitioningHandle from(ConnectorPartitioningHandle handle)
    {
        if (handle instanceof CombinedBucketedPartitioningHandle) {
            return (CombinedBucketedPartitioningHandle) handle;
        }
        if (handle instanceof BucketedIcebergPartitioningHandle) {
            BucketedIcebergPartitioningHandle bucketedPartitioningHandle = (BucketedIcebergPartitioningHandle) handle;
            return new CombinedBucketedPartitioningHandle(ImmutableMap.of(TableVersion.from(bucketedPartitioningHandle), bucketedPartitioningHandle), bucketedPartitioningHandle.getMaxCompatibleBucketCounts());
        }
        throw new UnsupportedOperationException(format("%s is not a supported ConnectorPartitioningHandle", handle.getClass()));
    }

    public static Optional<CombinedBucketedPartitioningHandle> makeCommonPartitioningHandle(ConnectorSession session, CombinedBucketedPartitioningHandle left, CombinedBucketedPartitioningHandle right)
    {
        checkArgument(left.tablePartitioningHandles.size() > 0, "CombinedBucketedPartitioningHandle cannot be empty");
        checkArgument(right.tablePartitioningHandles.size() > 0, "CombinedBucketedPartitioningHandle cannot be empty");
        int dimension = left.tablePartitioningHandles.values().stream().findFirst().get().getPartitioningColumns().size();
        // All
        if (!left.tablePartitioningHandles.values().stream().allMatch(handle -> handle.getPartitioningColumns().size() == dimension) || !right.tablePartitioningHandles.values().stream().allMatch(handle -> handle.getPartitioningColumns().size() == dimension)) {
            // all tablePartitioningHandles must be same dimension
            return Optional.empty();
        }
        // Same table version must have same bucketing scheme to be compatible
        for (TableVersion tableVersion : Sets.intersection(left.tablePartitioningHandles.keySet(), right.tablePartitioningHandles.keySet())) {
            BucketedIcebergPartitioningHandle leftHandle = left.tablePartitioningHandles.get(tableVersion);
            BucketedIcebergPartitioningHandle rightHandle = right.tablePartitioningHandles.get(tableVersion);
            if (!leftHandle.equals(rightHandle)) {
                return Optional.empty();
            }
        }
        List<Integer> maxCompatibleBucketCounts = new ArrayList<>(left.maxCompatibleBucketCounts);
        Stream.concat(Stream.concat(left.tablePartitioningHandles.values().stream().map(BucketedPartitioningHandle::getMaxCompatibleBucketCounts), right.tablePartitioningHandles.values().stream().map(BucketedPartitioningHandle::getMaxCompatibleBucketCounts)), Stream.of(right.maxCompatibleBucketCounts)).forEach(bucketCounts -> {
            checkArgument(maxCompatibleBucketCounts.size() == bucketCounts.size(), "maxCompatibleBucketCounts must have same size as bucketCounts");
            for (int i = 0; i < maxCompatibleBucketCounts.size(); i++) {
                maxCompatibleBucketCounts.set(i, Integer.min(maxCompatibleBucketCounts.get(i), bucketCounts.get(i)));
            }
        });
        boolean compatible = Stream.concat(Stream.concat(left.tablePartitioningHandles.values().stream().map(BucketedPartitioningHandle::getMaxCompatibleBucketCounts), right.tablePartitioningHandles.values().stream().map(BucketedPartitioningHandle::getMaxCompatibleBucketCounts)), Stream.of(right.maxCompatibleBucketCounts)).anyMatch(bucketCounts -> {
            for (int i = 0; i < maxCompatibleBucketCounts.size(); i++) {
                int compatibleBucketCount = maxCompatibleBucketCounts.get(i);
                int bucketCount = bucketCounts.get(i);
                if (compatibleBucketCount != bucketCount && !isOptimizedMismatchedBucketCount(session)) {
                    return false;
                }
                if (bucketCount % compatibleBucketCount != 0) {
                    return false;
                }
                if (Integer.bitCount(bucketCount / compatibleBucketCount) != 1) {
                    return false;
                }
            }
            return true;
        });
        if (!compatible) {
            return Optional.empty();
        }

        ImmutableMap.Builder<TableVersion, BucketedIcebergPartitioningHandle> combined = ImmutableMap.builder();
        Stream.concat(left.tablePartitioningHandles.entrySet().stream(), right.tablePartitioningHandles.entrySet().stream()).forEach(combined::put);

        return Optional.of(new CombinedBucketedPartitioningHandle(combined.buildKeepingLast(), ImmutableList.copyOf(maxCompatibleBucketCounts)));
    }

    public Optional<BucketedIcebergPartitioningHandle> getBucketedIcebergPartitioningHandle(IcebergTableHandle tableHandle)
    {
        return Optional.ofNullable(tablePartitioningHandles.get(new TableVersion(tableHandle.getSchemaTableName(), tableHandle.getSnapshotId())));
    }

    @JsonProperty
    public Map<TableVersion, BucketedIcebergPartitioningHandle> getTablePartitioningHandles()
    {
        return tablePartitioningHandles;
    }

    @JsonProperty
    @Override
    public List<Integer> getMaxCompatibleBucketCounts()
    {
        return maxCompatibleBucketCounts;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        CombinedBucketedPartitioningHandle that = (CombinedBucketedPartitioningHandle) o;
        return Objects.equals(maxCompatibleBucketCounts, that.maxCompatibleBucketCounts)
                && Objects.equals(tablePartitioningHandles, that.tablePartitioningHandles);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(tablePartitioningHandles, maxCompatibleBucketCounts);
    }

    public static class TableVersion
    {
        // We can add specId into TableVersion too
        // so that we can do bucketed join with read between same table but different partition specs.
        private static final Splitter SCHEMA_SPLITTER = Splitter.on('.');
        private static final Splitter VERSION_SPLITTER = Splitter.on('@');
        private final SchemaTableName tableName;
        private final Optional<Long> snapshotId;

        public TableVersion(SchemaTableName tableName, Optional<Long> snapshotId)
        {
            this.tableName = tableName;
            this.snapshotId = snapshotId;
        }

        @JsonCreator
        public static TableVersion parseTableVersion(String serializedTableVersion)
        {
            // not used in workers but need to serialize as part of FragmentedPlan
            requireNonNull(serializedTableVersion, "serializedTableVersion is null");
            List<String> parts = VERSION_SPLITTER.splitToList(serializedTableVersion);
            return new TableVersion(parseTableName(parts.get(0)), Optional.of(Long.parseLong(parts.get(1))));
        }

        private static SchemaTableName parseTableName(String schemaTableName)
        {
            checkArgument(!isNullOrEmpty(schemaTableName), "schemaTableName is null or is empty");
            List<String> parts = SCHEMA_SPLITTER.splitToList(schemaTableName);
            checkArgument(parts.size() == 2, "Invalid schemaTableName: %s", schemaTableName);
            return new SchemaTableName(parts.get(0), parts.get(1));
        }

        public static TableVersion from(BucketedIcebergPartitioningHandle handle)
        {
            return new TableVersion(handle.getTableName(), Optional.of(handle.getSnapshotId()));
        }

        @JsonValue
        public String getSerializedTableVersion()
        {
            return format("%s@%s", tableName.toString(), snapshotId.orElse(0L));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableVersion that = (TableVersion) o;
            return Objects.equals(snapshotId, that.snapshotId) && Objects.equals(tableName, that.tableName);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tableName, snapshotId);
        }
    }
}
