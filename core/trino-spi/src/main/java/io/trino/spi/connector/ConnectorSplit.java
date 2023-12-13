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
     * Lets the scheduler run this split on any node when the designated node is unavailable.
     * <p>
     * Ordinarily, when {@link #isRemotelyAccessible()} is false, the split must run on a node from the {@link #getAddresses()} list.  If that
     * node is down, the split will fail to run, and the query will be aborted.  Sometimes, however, it's desirable not to fail but to run the
     * split somewhere else.  This technically violates the meaning of {@link #isRemotelyAccessible()} in order to be fault-tolerant.
     * <p>
     * The behavior is controlled by this method.  When it returns false, the split will fail if its designated host dies.
     * But when this returns true, the split will still be run on some arbitrary node.
     *
     * @return True if the scheduler may schedule this split outside {@link #getAddresses()} (even if {@link #isRemotelyAccessible()} is false).
     *
     * @apiNote This method should not return false when {@link #isRemotelyAccessible()} returns true.  In that case, schedulers are free to run this split
     * anywhere, so it would be confusing if this suggested something different.  But when {@link #isRemotelyAccessible()} returns false,
     * this method can return either value, depending on whether failover is allowed or not.
     */
    default boolean isRemotelyAccessibleIfNodeMissing()
    {
        return isRemotelyAccessible();
    }

    Object getInfo();

    default SplitWeight getSplitWeight()
    {
        return SplitWeight.standard();
    }

    default long getRetainedSizeInBytes()
    {
        throw new UnsupportedOperationException("This connector does not provide memory accounting capabilities for ConnectorSplit");
    }
}
