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

import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.spi.type.Varchars.truncateToLength;

/**
 * Legacy {@code CHAR} to {@code VARCHAR} cast that re-pads the value to its declared {@code CHAR}
 * length. Registered in place of {@link CharToVarcharCast} when the
 * {@code deprecated.legacy-varchar-to-char-coercion} configuration property is set.
 */
public final class LegacyCharToVarcharCast
{
    private LegacyCharToVarcharCast() {}

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(@LiteralParameter("x") Long x, @LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        if (x.intValue() <= y.intValue()) {
            return padSpaces(slice, x.intValue());
        }
        return padSpaces(truncateToLength(slice, y.intValue()), y.intValue());
    }
}
