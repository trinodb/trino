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

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.RealType;
import io.trino.spi.type.VarcharType;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.RealType.REAL;
import static java.lang.String.format;

public final class FloatToVarcharCoercers
{
    private FloatToVarcharCoercers() {}

    public static TypeCoercer<RealType, VarcharType> createFloatToVarcharCoercer(VarcharType toType, boolean isOrcFile)
    {
        return isOrcFile ? new OrcFloatToVarcharCoercer(toType) : new FloatToVarcharCoercer(toType);
    }

    public static class FloatToVarcharCoercer
            extends TypeCoercer<RealType, VarcharType>
    {
        public FloatToVarcharCoercer(VarcharType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            writeFloatAsSlice(REAL.getFloat(block, position), blockBuilder, toType);
        }
    }

    public static class OrcFloatToVarcharCoercer
            extends TypeCoercer<RealType, VarcharType>
    {
        public OrcFloatToVarcharCoercer(VarcharType toType)
        {
            super(REAL, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            float floatValue = REAL.getFloat(block, position);

            if (Float.isNaN(floatValue)) {
                blockBuilder.appendNull();
                return;
            }
            writeFloatAsSlice(floatValue, blockBuilder, toType);
        }
    }

    private static void writeFloatAsSlice(float value, BlockBuilder blockBuilder, VarcharType varcharType)
    {
        Slice converted = Slices.utf8Slice(Float.toString(value));
        if (!varcharType.isUnbounded() && countCodePoints(converted) > varcharType.getBoundedLength()) {
            throw new TrinoException(INVALID_ARGUMENTS, format("Varchar representation of %s exceeds %s bounds", value, varcharType));
        }
        varcharType.writeSlice(blockBuilder, converted);
    }
}
