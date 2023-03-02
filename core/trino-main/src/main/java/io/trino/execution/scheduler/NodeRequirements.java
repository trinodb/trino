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
package io.trino.execution.scheduler;

import com.google.common.collect.ImmutableSet;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.CatalogHandle;

import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.airlift.slice.SizeOf.estimatedSizeOf;
import static io.airlift.slice.SizeOf.instanceSize;
import static io.airlift.slice.SizeOf.sizeOf;
import static java.util.Objects.requireNonNull;

public class NodeRequirements
{
    private static final int INSTANCE_SIZE = instanceSize(NodeRequirements.class);

    private final Optional<CatalogHandle> catalogHandle;
    private final Set<HostAddress> addresses;

    public NodeRequirements(Optional<CatalogHandle> catalogHandle, Set<HostAddress> addresses)
    {
        this.catalogHandle = requireNonNull(catalogHandle, "catalogHandle is null");
        this.addresses = ImmutableSet.copyOf(requireNonNull(addresses, "addresses is null"));
    }

    /*
     * If present constraint execution to nodes with the specified catalog installed
     */
    public Optional<CatalogHandle> getCatalogHandle()
    {
        return catalogHandle;
    }

    /*
     * Constrain execution to these nodes, if any
     */
    public Set<HostAddress> getAddresses()
    {
        return addresses;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NodeRequirements that = (NodeRequirements) o;
        return Objects.equals(catalogHandle, that.catalogHandle) && Objects.equals(addresses, that.addresses);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalogHandle, addresses);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("catalogHandle", catalogHandle)
                .add("addresses", addresses)
                .toString();
    }

    public long getRetainedSizeInBytes()
    {
        return INSTANCE_SIZE
                + sizeOf(catalogHandle, CatalogHandle::getRetainedSizeInBytes)
                + estimatedSizeOf(addresses, HostAddress::getRetainedSizeInBytes);
    }
}
