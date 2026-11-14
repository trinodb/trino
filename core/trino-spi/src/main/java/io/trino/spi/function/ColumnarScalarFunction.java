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
 * scalar function. The arguments are not restricted to the positions being
 * evaluated: {@code selectedPositions} identifies the positions of
 * {@code arguments} that must be evaluated, and the function must not read
 * argument values or raise failures for any other position.
 * <p>
 * The returned block must have {@code selectedPositions.size()} positions, and
 * position {@code i} of the result is the function value for argument position
 * {@code selectedPositions.position(i)}. An implementation that computes its
 * result over all argument positions, for example by reusing the structure of
 * its input blocks, can select the result positions with
 * {@link SelectedPositions#select(Block)}. Implementations are responsible for
 * the function's null and failure semantics.
 * <p>
 * Implementations must be thread safe and must not call
 * {@link SourcePage#selectPositions(int[], int, int)}.
 */
@Unstable
@FunctionalInterface
public interface ColumnarScalarFunction
{
    Block evaluate(ConnectorSession session, SourcePage arguments, SelectedPositions selectedPositions);
}
