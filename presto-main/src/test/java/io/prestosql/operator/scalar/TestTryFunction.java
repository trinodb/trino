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
package io.prestosql.operator.scalar;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.SqlDecimal;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.createVarcharType;
import static io.prestosql.type.JsonType.JSON;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.Arrays.asList;

public class TestTryFunction
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerScalar(getClass());
    }

    @ScalarFunction
    @SqlType("bigint")
    public static long throwError()
    {
        throw new PrestoException(GENERIC_INTERNAL_ERROR, "internal error, should not be suppressed by " + TryFunction.NAME);
    }

    @Test
    public void testBasic()
    {
        assertFunction(createTryExpression("42"), INTEGER, 42);
        assertFunction(createTryExpression("DOUBLE '4.5'"), DOUBLE, 4.5);
        assertFunction(createTryExpression("DECIMAL '4.5'"), createDecimalType(2, 1), SqlDecimal.of("4.5"));
        assertFunction(createTryExpression("TRUE"), BOOLEAN, true);
        assertFunction(createTryExpression("'hello'"), createVarcharType(5), "hello");
        assertFunction(createTryExpression("JSON '[true, false, 12, 12.7, \"12\", null]'"), JSON, "[true,false,12,12.7,\"12\",null]");
        assertFunction(createTryExpression("ARRAY [1, 2]"), new ArrayType(INTEGER), asList(1, 2));
        assertFunction(createTryExpression("NULL"), UNKNOWN, null);
    }

    @Test
    public void testExceptions()
    {
        // Exceptions that should be suppressed
        assertFunction(createTryExpression("1/0"), INTEGER, null);
        assertFunction(createTryExpression("JSON_PARSE('INVALID')"), JSON, null);
        assertFunction(createTryExpression("CAST(NULL AS INTEGER)"), INTEGER, null);
        assertFunction(createTryExpression("ABS(-9223372036854775807 - 1)"), BIGINT, null);

        // Exceptions that should not be suppressed
        assertInvalidFunction(createTryExpression("throw_error()"), GENERIC_INTERNAL_ERROR);
    }

    private static String createTryExpression(String expression)
    {
        return "\"" + TryFunction.NAME + "\"(() -> " + expression + ")";
    }
}
