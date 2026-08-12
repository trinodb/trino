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
package io.trino.spi.function;

import io.trino.spi.Unstable;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;

/**
 * Evaluates a scalar function for an entire column at a time.
 * <p>
 * Channel {@code i} in {@code arguments} contains argument {@code i} of the
 * scalar function. The arguments have already been restricted to the positions
 * being evaluated, and the returned block must have the same position count.
 * Implementations are responsible for the function's null and failure semantics.
 * <p>
 * Implementations must be thread safe and must not call
 * {@link SourcePage#selectPositions(int[], int, int)}.
 */
@Unstable
@FunctionalInterface
public interface ColumnarScalarFunctionImplementation
{
    Block evaluate(ConnectorSession session, SourcePage arguments);
}
