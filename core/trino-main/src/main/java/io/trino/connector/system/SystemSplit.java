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
package io.trino.connector.system;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static java.util.Objects.requireNonNull;

public class SystemSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SystemSplit.class);

    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> constraint;

    public SystemSplit(HostAddress address, TupleDomain<ColumnHandle> constraint)
    {
        this(ImmutableList.of(requireNonNull(address, "address is null")), constraint);
    }

    @JsonCreator
    public SystemSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint)
    {
        requireNonNull(addresses, "addresses is null");
        checkArgument(!addresses.isEmpty(), "addresses is empty");
        this.addresses = ImmutableList.copyOf(addresses);
        this.constraint = requireNonNull(constraint, "constraint is null");
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return false;
    }

    @Override
    @JsonProperty
    public List<HostAddress> getAddresses()
    {
        return addresses;
    }

    @JsonProperty
    public TupleDomain<ColumnHandle> getConstraint()
    {
        return constraint;
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
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + constraint.getRetainedSizeInBytes(columnHandle -> ((SystemColumnHandle) columnHandle).getRetainedSizeInBytes());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .toString();
    }
}
