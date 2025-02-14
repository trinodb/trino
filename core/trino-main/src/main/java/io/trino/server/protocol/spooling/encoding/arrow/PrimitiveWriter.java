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
package io.trino.server.protocol.spooling.encoding.arrow;

import io.trino.spi.block.Block;
import org.apache.arrow.vector.ValueVector;

import static java.util.Objects.requireNonNull;

public abstract sealed class PrimitiveWriter<V extends ValueVector>
        implements ArrowWriter
        permits FixedWidthWriter, NullWriter, VariableWidthWriter
{
    protected final V vector;

    protected PrimitiveWriter(V vector)
    {
        this.vector = requireNonNull(vector, "vector is null");
    }

    @Override
    public void write(Block block)
    {
        initialize(block);
        vector.setValueCount(block.getPositionCount());

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (block.isNull(position)) {
                setNull(position);
            }
        }

        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                writeValue(block, position);
            }
        }
    }

    protected abstract void initialize(Block block);

    protected abstract void setNull(int position);

    protected abstract void writeValue(Block block, int position);
}
