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
package io.trino.metadata;

public enum NodeState
{
    /**
     * Server is up and running ready to handle tasks
     */
    ACTIVE,
    /**
     * Never used internally, might be used by discoveryNodeManager when communication error occurs
     */
    INACTIVE,
    /**
     * A reversible graceful shutdown, can go to forward to DRAINED or back to ACTIVE.
     */
    DRAINING,
    /**
     * All tasks are finished, server can be safely and quickly stopped. Can also go back to ACTIVE.
     */
    DRAINED,
    /**
     * Graceful shutdown, non-reversible, when observed will drain and terminate
     */
    SHUTTING_DOWN
}
