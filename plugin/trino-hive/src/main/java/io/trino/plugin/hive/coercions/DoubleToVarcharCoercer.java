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
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.VarcharType;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static java.lang.String.format;

public class DoubleToVarcharCoercer
        extends TypeCoercer<DoubleType, VarcharType>
{
    private final boolean isOrcFile;

    public DoubleToVarcharCoercer(VarcharType toType, boolean isOrcFile)
    {
        super(DOUBLE, toType);
        this.isOrcFile = isOrcFile;
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        double doubleValue = DOUBLE.getDouble(block, position);

        if (Double.isNaN(doubleValue) && isOrcFile) {
            blockBuilder.appendNull();
            return;
        }

        Slice converted = Slices.utf8Slice(Double.toString(doubleValue));
        if (!toType.isUnbounded() && countCodePoints(converted) > toType.getBoundedLength()) {
            throw new TrinoException(INVALID_ARGUMENTS, format("Varchar representation of %s exceeds %s bounds", doubleValue, toType));
        }
        toType.writeSlice(blockBuilder, converted);
    }
}
