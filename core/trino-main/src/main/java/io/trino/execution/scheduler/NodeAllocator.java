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

import com.google.common.util.concurrent.ListenableFuture;
import io.trino.metadata.InternalNode;

import java.io.Closeable;

public interface NodeAllocator
        extends Closeable
{
    /**
     * Requests acquisition of node. Obtained node can be obtained via {@link NodeLease#getNode()} method.
     * The node may not be available immediately. Calling party needs to wait until future returned is done.
     *
     * It is obligatory for the calling party to release all the leases they obtained via {@link NodeLease#release()}.
     */
    NodeLease acquire(NodeRequirements requirements);

    @Override
    void close();

    interface NodeLease
    {
        ListenableFuture<InternalNode> getNode();

        void release();
    }
}
