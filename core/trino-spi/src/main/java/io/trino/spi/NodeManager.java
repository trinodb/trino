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
package io.trino.spi;

import io.trino.spi.connector.ConnectorContext;

import java.util.Set;

import static io.trino.spi.StandardErrorCode.NO_NODES_AVAILABLE;

public interface NodeManager
{
    Set<Node> getAllNodes();

    Set<Node> getWorkerNodes();

    /**
     * @deprecated Use {@link ConnectorContext#getCurrentNode()} instead.
     */
    @Deprecated
    Node getCurrentNode();

    default Set<Node> getRequiredWorkerNodes()
    {
        Set<Node> nodes = getWorkerNodes();
        if (nodes.isEmpty()) {
            throw new TrinoException(NO_NODES_AVAILABLE, "No nodes available to run query");
        }
        return nodes;
    }
}
