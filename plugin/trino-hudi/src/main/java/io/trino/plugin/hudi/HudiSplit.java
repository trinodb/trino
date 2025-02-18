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
package io.trino.plugin.hudi;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public record HudiSplit(
        String location,
        long start,
        long length,
        long fileSize,
        long fileModifiedTime,
        TupleDomain<HiveColumnHandle> predicate,
        List<HivePartitionKey> partitionKeys,
        SplitWeight splitWeight)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = toIntExact(instanceSize(HudiSplit.class));

    public HudiSplit
    {
        checkArgument(start >= 0, "start must be positive");
        checkArgument(length >= 0, "length must be positive");
        checkArgument(start + length <= fileSize, "fileSize must be at least start + length");

        requireNonNull(location, "location is null");
        requireNonNull(predicate, "predicate is null");
        partitionKeys = ImmutableList.copyOf(partitionKeys);
        requireNonNull(splitWeight, "splitWeight is null");
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("location", location)
                .put("start", String.valueOf(start))
                .put("length", String.valueOf(length))
                .put("fileSize", String.valueOf(fileSize))
                .put("fileModifiedTime", String.valueOf(fileModifiedTime))
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(location)
                + splitWeight.getRetainedSizeInBytes()
                + predicate.getRetainedSizeInBytes(HiveColumnHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(partitionKeys, HivePartitionKey::estimatedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(location)
                .addValue(start)
                .addValue(length)
                .addValue(fileSize)
                .addValue(fileModifiedTime)
                .toString();
    }
}
