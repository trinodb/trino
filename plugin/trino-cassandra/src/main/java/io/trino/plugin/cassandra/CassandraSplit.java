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
package io.trino.plugin.cassandra;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public record CassandraSplit(String partitionId, String splitCondition, List<HostAddress> addresses)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(CassandraSplit.class);

    public CassandraSplit
    {
        requireNonNull(partitionId, "partitionId is null");
        addresses = ImmutableList.copyOf(addresses);
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.<String, String>builder()
                .put("hosts", addresses.stream().map(HostAddress::toString).collect(joining(",")))
                .put("partitionId", partitionId)
                .buildOrThrow();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(partitionId)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + estimatedSizeOf(splitCondition);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(partitionId)
                .toString();
    }

    public String getWhereClause()
    {
        if (partitionId.equals(CassandraPartition.UNPARTITIONED_ID)) {
            if (splitCondition != null) {
                return " WHERE " + splitCondition;
            }
            return "";
        }
        if (splitCondition != null) {
            return " WHERE " + partitionId + " AND " + splitCondition;
        }
        return " WHERE " + partitionId;
    }
}
