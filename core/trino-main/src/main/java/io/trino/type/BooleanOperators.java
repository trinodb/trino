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
package io.trino.type;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.ScalarOperator;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import static io.trino.spi.function.OperatorType.CAST;
import static java.lang.Float.floatToRawIntBits;
import static java.nio.charset.StandardCharsets.US_ASCII;

public final class BooleanOperators
{
    private static final Slice TRUE = Slices.copiedBuffer("true", US_ASCII);
    private static final Slice FALSE = Slices.copiedBuffer("false", US_ASCII);

    private BooleanOperators()
    {
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DOUBLE)
    public static double castToDouble(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.REAL)
    public static long castToReal(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? floatToRawIntBits(1.0f) : floatToRawIntBits(0.0f);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.BIGINT)
    public static long castToBigint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.INTEGER)
    public static long castToInteger(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.SMALLINT)
    public static long castToSmallint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TINYINT)
    public static long castToTinyint(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? 1 : 0;
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToVarchar(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return value ? TRUE : FALSE;
    }

    @SqlType(StandardTypes.BOOLEAN)
    @ScalarFunction(hidden = true) // TODO: this should not be callable from SQL
    public static boolean not(@SqlType(StandardTypes.BOOLEAN) boolean value)
    {
        return !value;
    }
}
