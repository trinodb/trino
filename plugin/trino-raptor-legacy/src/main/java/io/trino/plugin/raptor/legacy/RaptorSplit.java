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
package io.trino.plugin.raptor.legacy;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.OptionalInt;
import java.util.Set;
import java.util.UUID;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class RaptorSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(RaptorSplit.class);
    private static final int UUID_INSTANCE_SIZE = instanceSize(UUID.class);

    private final Set<UUID> shardUuids;
    private final OptionalInt bucketNumber;
    private final List<HostAddress> addresses;

    @JsonCreator
    public RaptorSplit(
            @JsonProperty("shardUuids") Set<UUID> shardUuids,
            @JsonProperty("bucketNumber") OptionalInt bucketNumber)
    {
        this(shardUuids, bucketNumber, ImmutableList.of());
    }

    public RaptorSplit(UUID shardUuid, List<HostAddress> addresses)
    {
        this(ImmutableSet.of(shardUuid), OptionalInt.empty(), addresses);
    }

    public RaptorSplit(Set<UUID> shardUuids, int bucketNumber, HostAddress address)
    {
        this(shardUuids, OptionalInt.of(bucketNumber), ImmutableList.of(address));
    }

    private RaptorSplit(Set<UUID> shardUuids, OptionalInt bucketNumber, List<HostAddress> addresses)
    {
        this.shardUuids = ImmutableSet.copyOf(requireNonNull(shardUuids, "shardUuids is null"));
        this.bucketNumber = requireNonNull(bucketNumber, "bucketNumber is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public Set<UUID> getShardUuids()
    {
        return shardUuids;
    }

    @JsonProperty
    public OptionalInt getBucketNumber()
    {
        return bucketNumber;
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(shardUuids, value -> UUID_INSTANCE_SIZE)
                + sizeOf(bucketNumber)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("shardUuids", shardUuids)
                .add("bucketNumber", bucketNumber.isPresent() ? bucketNumber.getAsInt() : null)
                .add("hosts", addresses)
                .omitNullValues()
                .toString();
    }
}
