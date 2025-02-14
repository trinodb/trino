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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.SizeOf;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.predicate.TupleDomain;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public class SystemSplit
        implements ConnectorSplit
{
    private static final int INSTANCE_SIZE = instanceSize(SystemSplit.class);

    private final List<HostAddress> addresses;
    private final TupleDomain<ColumnHandle> constraint;
    private final Optional<String> catalogName;

    public SystemSplit(HostAddress address, TupleDomain<ColumnHandle> constraint, Optional<String> catalogName)
    {
        this(ImmutableList.of(requireNonNull(address, "address is null")), constraint, catalogName);
    }

    @JsonCreator
    public SystemSplit(
            @JsonProperty("addresses") List<HostAddress> addresses,
            @JsonProperty("constraint") TupleDomain<ColumnHandle> constraint,
            @JsonProperty("catalogName") Optional<String> catalogName)
    {
        requireNonNull(addresses, "addresses is null");
        checkArgument(!addresses.isEmpty(), "addresses is empty");
        this.addresses = ImmutableList.copyOf(addresses);
        this.constraint = requireNonNull(constraint, "constraint is null");
        this.catalogName = requireNonNull(catalogName, "catalogName is null");
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

    @JsonProperty
    public Optional<String> getCatalogName()
    {
        return catalogName;
    }

    @Override
    public Map<String, String> getSplitInfo()
    {
        return ImmutableMap.of("addresses", addresses.stream().map(HostAddress::toString).collect(joining(",")));
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes)
                + constraint.getRetainedSizeInBytes(columnHandle -> ((SystemColumnHandle) columnHandle).getRetainedSizeInBytes())
                + sizeOf(catalogName, SizeOf::estimatedSizeOf);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("addresses", addresses)
                .toString();
    }
}
