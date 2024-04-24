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
package io.trino.plugin.hive.coercions;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.type.RealType.REAL;

public class VarcharToFloatCoercer
        extends TypeCoercer<VarcharType, RealType>
{
    private final boolean isOrcFile;

    public VarcharToFloatCoercer(VarcharType fromType, boolean isOrcFile)
    {
        super(fromType, REAL);
        this.isOrcFile = isOrcFile;
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        float floatValue;
        try {
            floatValue = Float.parseFloat(fromType.getSlice(block, position).toStringUtf8());
        }
        catch (NumberFormatException e) {
            blockBuilder.appendNull();
            return;
        }

        // Apache Hive reads Float.NaN as null when coerced to varchar for ORC file format
        if (Float.isNaN(floatValue) && isOrcFile) {
            blockBuilder.appendNull();
            return;
        }
        REAL.writeFloat(blockBuilder, floatValue);
    }
}
