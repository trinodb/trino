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
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;

import static io.trino.spi.type.Chars.truncateToLengthAndTrimSpaces;
import static java.lang.Math.toIntExact;

/**
 * Default saturated floor cast from {@code VARCHAR} to {@code CHAR}: returns the largest
 * {@code char(y)} value (in {@code CHAR} ordering, which compares values as if space-padded to
 * their declared length) that, cast back to varchar ({@link CharToVarcharCast}, which returns the
 * value unpadded), does not exceed the input. Truncating to {@code y} code points and trimming
 * trailing spaces yields exactly that value, so this operator coincides with
 * {@link CharacterStringCasts#varcharToCharCast}. Registered unless the
 * {@code deprecated.legacy-varchar-to-char-coercion} configuration property is set, in which case
 * {@link LegacyVarcharToCharSaturatedFloorCast} is registered instead.
 */
public final class VarcharToCharSaturatedFloorCast
{
    private VarcharToCharSaturatedFloorCast() {}

    @ScalarOperator(OperatorType.SATURATED_FLOOR_CAST)
    @SqlType("char(y)")
    @LiteralParameters({"x", "y"})
    public static Slice varcharToCharSaturatedFloorCast(@LiteralParameter("y") long y, @SqlType("varchar(x)") Slice slice)
    {
        return truncateToLengthAndTrimSpaces(slice, toIntExact(y));
    }
}
