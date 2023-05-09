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

import java.util.Random;

/**
 * Represents a handle to a relation returned from the connector to the engine.
 * It will be used by the engine whenever given relation will be accessed.
 */
public interface ConnectorTableHandle
{
    /**
     * Returns true if a ConnectorTableHandle is "fusible" with another ConnectorTableHandler.
     * Fusible here means the two tables can possibly be optimized by SharedScanOptimizer.
     */
    default boolean equalsForFusion(Object o)
    {
        return false;
    }

    /**
     * Returns the hashcode that is & should only be used by Query Fusion to compare the table handles and group them into HashMaps.
     */
    default int hashCodeForFusion()
    {
        Random rand = new Random();
        return rand.nextInt();
    }
}
