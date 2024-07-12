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
package io.trino.plugin.memory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Map;
import java.util.OptionalLong;

import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public record MemorySplit(
        long table,
        // part of the pages on one worker that this splits is responsible
        int partNumber,
        // how many concurrent reads there will be from one worker
        int totalPartsPerWorker,
        HostAddress address,
        long expectedRows,
        OptionalLong limit)
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(MemorySplit.class);

    public MemorySplit(
            long table,
            int partNumber,
            int totalPartsPerWorker,
            HostAddress address,
            long expectedRows,
            OptionalLong limit)
    {
        checkState(partNumber >= 0, "partNumber must be >= 0");
        checkState(totalPartsPerWorker >= 1, "totalPartsPerWorker must be >= 1");
        checkState(totalPartsPerWorker > partNumber, "totalPartsPerWorker must be > partNumber");

        this.table = table;
        this.partNumber = partNumber;
        this.totalPartsPerWorker = totalPartsPerWorker;
        this.address = requireNonNull(address, "address is null");
        this.expectedRows = expectedRows;
        this.limit = limit;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of("table", String.valueOf(table), "partNumber", String.valueOf(partNumber), "address", address.toString());
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + address.getRetainedSizeInBytes()
                + sizeOf(limit);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of(address);
    }
}
