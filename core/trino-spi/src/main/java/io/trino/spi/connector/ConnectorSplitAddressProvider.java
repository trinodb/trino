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

import java.util.List;

/**
 * Provides address (locality) information for scheduling {@link ConnectorSplit}s.
 */
public interface ConnectorSplitAddressProvider
{
    /**
     * Returns the list of preferred or required host addresses for the given split.
     * <p>
     * When {@link #isRemotelyAccessible(ConnectorSplit)} returns {@code true}, these addresses
     * are hints: the scheduler will try them first but may fall back to any available node.
     * When it returns {@code false}, the split <em>must</em> run on one of the returned addresses.
     */
    List<HostAddress> getAddresses(ConnectorSplit split);

    /**
     * Returns {@code true} when the split can be scheduled on any node (addresses are hints),
     * or {@code false} when the split must be scheduled on one of the addresses returned by
     * {@link #getAddresses(ConnectorSplit)}.
     */
    boolean isRemotelyAccessible(ConnectorSplit split);

    ConnectorSplitAddressProvider DEFAULT = new ConnectorSplitAddressProvider()
    {
        @Override
        public List<HostAddress> getAddresses(ConnectorSplit split)
        {
            return List.of();
        }

        @Override
        public boolean isRemotelyAccessible(ConnectorSplit split)
        {
            return true;
        }
    };
}
