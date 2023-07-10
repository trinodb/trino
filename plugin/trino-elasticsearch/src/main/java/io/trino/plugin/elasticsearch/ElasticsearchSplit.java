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
package io.trino.plugin.elasticsearch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class ElasticsearchSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ElasticsearchSplit.class);

    private final String index;
    private final int shard;
    private final Optional<String> address;

    @JsonCreator
    public ElasticsearchSplit(
            @JsonProperty("index") String index,
            @JsonProperty("shard") int shard,
            @JsonProperty("address") Optional<String> address)
    {
        this.index = requireNonNull(index, "index is null");
        this.shard = shard;
        this.address = requireNonNull(address, "address is null");
    }

    @JsonProperty
    public String getIndex()
    {
        return index;
    }

    @JsonProperty
    public int getShard()
    {
        return shard;
    }

    @JsonProperty
    public Optional<String> getAddress()
    {
        return address;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return address.map(host -> ImmutableList.of(HostAddress.fromString(host)))
                .orElseGet(ImmutableList::of);
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
                + estimatedSizeOf(index)
                + sizeOf(address, SizeOf::estimatedSizeOf);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("index", index)
                .add("shard", shard)
                .toString();
    }
}
