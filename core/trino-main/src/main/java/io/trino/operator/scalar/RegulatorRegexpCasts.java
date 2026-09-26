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

import io.airlift.regulator.RegexpParseException;
import io.airlift.regulator.TrinoRegexp;
import io.airlift.slice.Slice;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.type.RegulatorRegexp;
import io.trino.type.RegulatorRegexpType;

import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Chars.padSpaces;
import static java.lang.Math.toIntExact;

public final class RegulatorRegexpCasts
{
    private RegulatorRegexpCasts() {}

    // fallible
    @LiteralParameters("x")
    @ScalarOperator(OperatorType.CAST)
    @SqlType(RegulatorRegexpType.NAME)
    public static RegulatorRegexp castVarcharToRegulatorRegexp(@SqlType("varchar(x)") Slice pattern)
    {
        return regulatorRegexp(pattern);
    }

    // fallible
    @ScalarOperator(OperatorType.CAST)
    @LiteralParameters("x")
    @SqlType(RegulatorRegexpType.NAME)
    public static RegulatorRegexp castCharToRegulatorRegexp(@LiteralParameter("x") long charLength, @SqlType("char(x)") Slice pattern)
    {
        return regulatorRegexp(padSpaces(pattern, toIntExact(charLength)));
    }

    public static RegulatorRegexp regulatorRegexp(Slice pattern)
    {
        try {
            return new RegulatorRegexp(pattern, TrinoRegexp.compile(pattern));
        }
        catch (RegexpParseException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
