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

import static io.trino.spi.type.Varchars.truncateToLength;

/**
 * Default {@code CHAR} to {@code VARCHAR} cast. Registered unless the
 * {@code deprecated.legacy-varchar-to-char-coercion} configuration property is set, in which case
 * {@link LegacyCharToVarcharCast} is registered instead.
 */
public final class CharToVarcharCast
{
    private CharToVarcharCast() {}

    @ScalarOperator(value = OperatorType.CAST, neverFails = true)
    @SqlType("varchar(y)")
    @LiteralParameters({"x", "y"})
    public static Slice charToVarcharCast(@LiteralParameter("y") Long y, @SqlType("char(x)") Slice slice)
    {
        // CHAR values are stored without trailing spaces. Casting to VARCHAR yields the unpadded value
        // (trailing spaces are not re-introduced), truncating only when the target VARCHAR is shorter.
        return truncateToLength(slice, y.intValue());
    }
}
