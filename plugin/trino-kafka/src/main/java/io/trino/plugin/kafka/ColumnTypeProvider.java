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
package io.trino.plugin.kafka;

import io.trino.spi.type.Type;

import java.util.Optional;

/**
 * The ColumnTypeProvider interface is responsible for providing type information
 * for columns represented by ColumnHandle instances. It plays a crucial role in the
 * ConstraintExtractor class for extracting TupleDomain from constraints.
 *
 * <p>Implementations of this interface should be capable of mapping a ColumnHandle
 * to its corresponding Type. This information is essential for handling expressions
 * and constraints involving columns in a connector.
 *
 * @param <C> The type of ColumnHandle or its subclass that this provider supports.
 *            It ensures that the ColumnHandle passed to the methods is of the correct
 *            type or its subclasses.
 */
public interface ColumnTypeProvider<C>
{
    /**
     * Retrieves the Type information for the given column represented by the provided
     * ColumnHandle.
     *
     * @param column The handle representing a column.
     * @return The Type of the specified column.
     * @throws UnsupportedOperationException if the type information for the column
     *         represented by the provided handle is not supported.
     */
    Optional<Type> getType(C column)
            throws UnsupportedOperationException;
}
