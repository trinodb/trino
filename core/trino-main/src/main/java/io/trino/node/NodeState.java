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

public enum NodeState
{
    /**
     * The node is up and ready to handle tasks.
     */
    ACTIVE,
    /**
     * The node is currently not handling tasks, but it is still part of the cluster.
     * This is an internal state used by node manager when communication errors occur.
     */
    INACTIVE,
    /**
     * A reversible graceful shutdown can go to forward to DRAINED or back to ACTIVE.
     */
    DRAINING,
    /**
     * All tasks are finished, server can be safely and quickly stopped. Can also go back to ACTIVE.
     */
    DRAINED,
    /**
     * Graceful shutdown, non-reversible, when observed will drain and terminate
     */
    SHUTTING_DOWN,
    /**
     * The node is not valid for this cluster. Nodes in this state are not visible to the node manager.
     * This is an internal state used by node manager when the environment or version of the node is
     * not valid for the cluster.
     */
    INVALID,
    /**
     * Connections to the node have been refused. Nodes in this state are not visible to the node manager.
     * This is an internal state used to by execution engine to produce better error messages.
     */
    GONE,
}
