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
package io.trino.plugin.ml;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.function.Description;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.type.BigintType.BIGINT;

public final class PowerOfTwoFunction
{
    private PowerOfTwoFunction() {}

    @Description("Parse flag as a list of numbers")
    @ScalarFunction("analyze_flag")
    @SqlType("array(bigint)")
    public static Block analyzeFlag(@SqlType(StandardTypes.BIGINT) long number)
    {
        BlockBuilder parts = BIGINT.createBlockBuilder(null, 10, Long.BYTES);
        int exponent = 0;

        while (number > 0) {
            if ((number & 1) == 1) {
                BIGINT.writeLong(parts, 1L << exponent);
            }
            number >>= 1;  // Shift right by 1
            exponent++;
        }

        return parts.build();
    }
}
