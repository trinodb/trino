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

import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.Type;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.type.DoubleType.DOUBLE;

public class IntegerNumberToDoubleCoercer<F extends Type>
        extends TypeCoercer<F, DoubleType>
{
    private static final long MIN_EXACT_DOUBLE = -(1L << 52); // -2^52
    private static final long MAX_EXACT_DOUBLE = (1L << 52) - 1; // 2^52 - 1

    public IntegerNumberToDoubleCoercer(F fromType)
    {
        super(fromType, DOUBLE);
    }

    @Override
    protected void applyCoercedValue(BlockBuilder blockBuilder, Block block, int position)
    {
        long value = fromType.getLong(block, position);
        // IEEE 754 double-precision can guarantee this for up to 53 bits (52 bits of significand + the implicit leading 1 bit)
        // https://stackoverflow.com/questions/43655668/are-all-integer-values-perfectly-represented-as-doubles
        if (overflow(value)) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, "Cannot read value '%s' as DOUBLE".formatted(value));
        }
        DOUBLE.writeDouble(blockBuilder, fromType.getLong(block, position));
    }

    private static boolean overflow(long value)
    {
        return value < MIN_EXACT_DOUBLE || value > MAX_EXACT_DOUBLE;
    }
}
