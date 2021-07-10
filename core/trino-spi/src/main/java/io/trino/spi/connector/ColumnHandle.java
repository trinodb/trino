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
 * Represents a handle to a column within a {@link ConnectorTableHandle} returned from the connector to the engine.
 * It will be used by the engine whenever given column will be accessed.
 * <p>
 * ColumnHandle should follow:
 * <ul>
 *     <li>a table cannot have two equal column handles</li>
 *     <li>column handle is unique within a table scope. Two different tables can have two equal column handles</li>
 * </ul>
 */
public interface ColumnHandle
{
    @Override
    int hashCode();

    @Override
    boolean equals(Object other);
}
