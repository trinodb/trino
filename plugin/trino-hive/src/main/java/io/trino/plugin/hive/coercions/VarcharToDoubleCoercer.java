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
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;

import static io.trino.spi.type.DoubleType.DOUBLE;

public class VarcharToDoubleCoercer
        extends TypeCoercer<VarcharType, DoubleType>
{
    private final boolean treatNaNAsNull;

    public VarcharToDoubleCoercer(VarcharType fromType, boolean treatNaNAsNull)
    {
        super(fromType, DOUBLE);
        this.treatNaNAsNull = treatNaNAsNull;
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        double doubleValue;
        try {
            doubleValue = Double.parseDouble(fromType.getSlice(block, position).toStringUtf8());
        }
        catch (NumberFormatException e) {
            blockBuilder.appendNull();
            return;
        }

        if (Double.isNaN(doubleValue) && treatNaNAsNull) {
            blockBuilder.appendNull();
            return;
        }
        DOUBLE.writeDouble(blockBuilder, doubleValue);
    }
}
