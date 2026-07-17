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
package io.trino.spi.connector;

import io.trino.spi.HostAddress;
import io.trino.spi.SplitWeight;

import java.util.List;
import java.util.Optional;

public interface ConnectorSplit
{
    /**
     * Returns true when this ConnectorSplit can be scheduled on any node.
     * <p>
     * When true, the addresses returned by {@link #getAddresses()} may be used as hints by the scheduler
     * during splits assignment.
     * When false, the split will always be scheduled on one of the addresses returned by {@link #getAddresses()}.
     */
    default boolean isRemotelyAccessible()
    {
        return true;
    }

    default List<HostAddress> getAddresses()
    {
        if (!isRemotelyAccessible()) {
            throw new IllegalStateException("getAddresses must be implemented when for splits with isRemotelyAccessible=false");
        }
        return List.of();
    }

    /**
     * Returns an optional affinity key so splits reading related content are routed to the
     * same worker(s) across queries. When empty, scheduling falls back to {@link #getAddresses()}.
     * <p>
     * Only remotely accessible splits may supply an affinity key (see {@link #isRemotelyAccessible()}).
     */
    default Optional<String> getAffinityKey()
    {
        return Optional.empty();
    }

    default SplitWeight getSplitWeight()
    {
        return SplitWeight.standard();
    }

    default long getRetainedSizeInBytes()
    {
        throw new UnsupportedOperationException("This connector does not provide memory accounting capabilities for ConnectorSplit");
    }
}
