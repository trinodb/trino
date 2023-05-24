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
package io.trino.plugin.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.plugin.thrift.api.TrinoThriftId;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.util.List;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class ThriftConnectorSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(ThriftConnectorSplit.class);

    private final TrinoThriftId splitId;
    private final List<HostAddress> addresses;

    @JsonCreator
    public ThriftConnectorSplit(
            @JsonProperty("splitId") TrinoThriftId splitId,
            @JsonProperty("addresses") List<HostAddress> addresses)
    {
        this.splitId = requireNonNull(splitId, "splitId is null");
        this.addresses = ImmutableList.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    @JsonProperty
    public TrinoThriftId getSplitId()
    {
        return splitId;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public Object getInfo()
    {
        return "";
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + splitId.getRetainedSizeInBytes()
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        ThriftConnectorSplit other = (ThriftConnectorSplit) obj;
        return Objects.equals(this.splitId, other.splitId) &&
                Objects.equals(this.addresses, other.addresses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(splitId, addresses);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("splitId", splitId)
                .add("addresses", addresses)
                .toString();
    }
}
