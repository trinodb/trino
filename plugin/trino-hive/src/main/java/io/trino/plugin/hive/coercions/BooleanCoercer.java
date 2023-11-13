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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;

import static io.airlift.slice.SliceUtf8.countCodePoints;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static java.lang.String.format;

public final class BooleanCoercer
{
    private static final Slice TRUE = utf8Slice("TRUE");
    private static final Slice FALSE = utf8Slice("FALSE");

    private BooleanCoercer() {}

    public static class BooleanToVarcharCoercer
            extends TypeCoercer<BooleanType, VarcharType>
    {
        public BooleanToVarcharCoercer(VarcharType toType)
        {
            super(BOOLEAN, toType);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            boolean value = BOOLEAN.getBoolean(block, position);
            Slice converted = value ? TRUE : FALSE;
            if (!toType.isUnbounded() && countCodePoints(converted) > toType.getBoundedLength()) {
                throw new TrinoException(INVALID_ARGUMENTS, format("Varchar representation of %s exceeds %s bounds", value, toType));
            }
            toType.writeSlice(blockBuilder, converted);
        }
    }
}
