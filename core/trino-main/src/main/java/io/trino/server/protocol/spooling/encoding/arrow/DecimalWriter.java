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
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import org.apache.arrow.vector.DecimalVector;

import static java.util.Objects.requireNonNull;

public final class DecimalWriter
        extends FixedWidthWriter<DecimalVector>
{
    private final DecimalType type;

    public DecimalWriter(DecimalVector vector, DecimalType type)
    {
        super(vector);
        this.type = requireNonNull(type, "type is null");
    }

    @Override
    protected void setNull(int position)
    {
        vector.setNull(position);
    }

    @Override
    protected void writeValue(Block block, int position)
    {
        if (type.isShort()) {
            vector.set(position, type.getLong(block, position));
        }
        else {
            Int128 decimal = (Int128) type.getObject(block, position);
            vector.setBigEndianSafe(position, decimal.toBigEndianBytes());
        }
    }
}
