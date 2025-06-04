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

/**
 * Represents a handle to a partitioning scheme used by the connector.
 * <p>
 * If the ConnectorPartitioningHandle of two tables are equal, the tables are guaranteed
 * to have the same partitioning scheme across nodes, and the engine may use a colocated join.
 */
public interface ConnectorPartitioningHandle
{
    default boolean isSingleNode()
    {
        return false;
    }

    default boolean isCoordinatorOnly()
    {
        return false;
    }
}
