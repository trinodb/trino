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
package io.trino.node;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public interface Announcer
{
    Announcer NOOP = new Announcer()
    {
        @Override
        public void start() {}

        @Override
        public ListenableFuture<?> forceAnnounce()
        {
            return Futures.immediateFuture(null);
        }

        @Override
        public void stop() {}
    };

    /**
     * Announce the existence of this node to the cluster.
     * This method should be called after the node is fully initialized.
     */
    void start();

    /**
     * Force an announcement of this node to the cluster.
     * This method can be used to refresh the node's presence in the cluster.
     *
     * @return a Future that completes when the announcement is done
     */
    ListenableFuture<?> forceAnnounce();

    /**
     * Stop announcing the existence of this node to the cluster.
     * This method should be called before the node is shut down.
     */
    void stop();
}
