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
package io.trino.plugin.iceberg.aggregation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.trino.plugin.iceberg.IcebergSplit;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;

public class AggregateIcebergSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(IcebergSplit.class);
    private final List<HostAddress> addresses;
    private final long totalCount;

    @JsonCreator
    public AggregateIcebergSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("totalCount") long totalCount)
    {
        this.addresses = addresses;
        this.totalCount = totalCount;
    }

    @JsonProperty
    public long getTotalCount()
    {
        return totalCount;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE + sizeOf(OptionalLong.of(totalCount));
    }

    @Override

    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @JsonProperty
    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return ImmutableMap.builder()
                .put("totalCount", totalCount)
                .buildOrThrow();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(totalCount)
                .toString();
    }
}
