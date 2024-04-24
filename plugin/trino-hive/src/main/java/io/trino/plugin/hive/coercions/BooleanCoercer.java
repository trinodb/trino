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

import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.VarcharType;

import java.util.Set;

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

    public static TypeCoercer<VarcharType, BooleanType> createVarcharToBooleanCoercer(VarcharType fromType, boolean isOrcFile)
    {
        return isOrcFile ? new OrcVarcharToBooleanCoercer(fromType) : new VarcharToBooleanCoercer(fromType);
    }

    public static class VarcharToBooleanCoercer
            extends TypeCoercer<VarcharType, BooleanType>
    {
        // These values are compatible with Hive 3.0, and they are populated from
        // https://github.com/apache/hive/blob/4df4d75bf1e16fe0af75aad0b4179c34c07fc975/serde/src/java/org/apache/hadoop/hive/serde2/objectinspector/primitive/PrimitiveObjectInspectorUtils.java#L559
        private static final Set<String> FALSE_VALUES = ImmutableSet.of(
                "false",
                "off",
                "no",
                "0",
                "");

        public VarcharToBooleanCoercer(VarcharType fromType)
        {
            super(fromType, BOOLEAN);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            toType.writeBoolean(blockBuilder, parseBoolean(fromType.getSlice(block, position).toStringUtf8()));
        }

        private boolean parseBoolean(String value)
        {
            for (String falseValue : FALSE_VALUES) {
                if (value.equalsIgnoreCase(falseValue)) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class OrcVarcharToBooleanCoercer
            extends TypeCoercer<VarcharType, BooleanType>
    {
        public OrcVarcharToBooleanCoercer(VarcharType fromType)
        {
            super(fromType, BOOLEAN);
        }

        @Override
        protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
        {
            try {
                // Apache Hive reads 0 as false, numeric string as true and non-numeric string as null for ORC file format
                // https://github.com/apache/orc/blob/fb1c4cb9461d207db652fc253396e57640ed805b/java/core/src/java/org/apache/orc/impl/ConvertTreeReaderFactory.java#L567
                toType.writeBoolean(blockBuilder, !(Long.parseLong(fromType.getSlice(block, position).toStringUtf8()) == 0));
            }
            catch (NumberFormatException e) {
                blockBuilder.appendNull();
            }
        }
    }
}
