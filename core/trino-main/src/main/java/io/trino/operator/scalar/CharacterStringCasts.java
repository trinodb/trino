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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.trino.operator.scalar.preimage.OrderPreservingCastPreimage;
import io.trino.operator.scalar.preimage.VarcharToCharPreimage;
import io.trino.spi.function.FunctionPreimage;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static io.trino.spi.type.Varchars.truncateToLength;
import static java.lang.Math.toIntExact;

public final class CharacterStringCasts
{
    private CharacterStringCasts() {}

    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToVarcharCast(@LiteralParameter("x") long x, @LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLength(slice, toIntExact(y));
        }
        return slice;
    }

    @FunctionPreimage(OrderPreservingCastPreimage.class)
    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToCharCast(@LiteralParameter("x") long x, @LiteralParameter("y") long y, @SqlType("char(x)") Slice slice)
    {
        if (x > y) {
            return truncateToLengthAndTrimSpaces(slice, toIntExact(y));
        }
        return slice;
    }

    @FunctionPreimage(VarcharToCharPreimage.class)
    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        return truncateToLengthAndTrimSpaces(slice, toIntExact(y));
    }
}
