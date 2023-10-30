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
package io.trino.plugin.iceberg.functions.tablechanges;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.plugin.iceberg.IcebergFileFormat;
import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static java.util.Objects.requireNonNull;

public record TableChangesSplit(
        ChangeType changeType,
        long snapshotId,
        long snapshotTimestamp,
        int changeOrdinal,
        String path,
        long start,
        long length,
        long fileSize,
        long fileRecordCount,
        IcebergFileFormat fileFormat,
        String partitionSpecJson,
        String partitionDataJson,
        SplitWeight splitWeight) implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = SizeOf.instanceSize(TableChangesSplit.class);

    public TableChangesSplit
    {
        requireNonNull(changeType, "changeType is null");
        requireNonNull(path, "path is null");
        requireNonNull(fileFormat, "fileFormat is null");
        requireNonNull(partitionSpecJson, "partitionSpecJson is null");
        requireNonNull(partitionDataJson, "partitionDataJson is null");
        requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public SplitWeight getSplitWeight()
    {
        return splitWeight;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("path", path)
                .put("start", start)
                .put("length", length)
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(path)
                + estimatedSizeOf(partitionSpecJson)
                + estimatedSizeOf(partitionDataJson)
                + splitWeight.getRetainedSizeInBytes();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(path)
                .add("start", start)
                .add("length", length)
                .add("records", fileRecordCount)
                .toString();
    }

    public enum ChangeType {
        ADDED_FILE("insert"),
        DELETED_FILE("delete"),
        POSITIONAL_DELETE("delete");

        private final String tableValue;

        ChangeType(String tableValue)
        {
            this.tableValue = tableValue;
        }

        public String getTableValue()
        {
            return tableValue;
        }
    }
}
