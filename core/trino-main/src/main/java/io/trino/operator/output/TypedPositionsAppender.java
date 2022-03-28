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
package io.trino.operator.output;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import static java.util.Objects.requireNonNull;

public class TypedPositionsAppender
        implements PositionsAppender
{
    private final Type type;

    public TypedPositionsAppender(Type type)
    {
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    public void appendTo(IntArrayList positions, Block source, BlockBuilder target)
    {
        int[] positionArray = positions.elements();
        for (int i = 0; i < positions.size(); i++) {
            type.appendTo(source, positionArray[i], target);
        }
    }
}
