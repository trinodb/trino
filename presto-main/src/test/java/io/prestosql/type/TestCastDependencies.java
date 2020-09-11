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
package io.prestosql.type;

import io.airlift.slice.Slice;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.CastDependency;
import io.prestosql.spi.function.OperatorDependency;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.StandardTypes;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.invoke.MethodHandle;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.function.OperatorType.EQUAL;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestCastDependencies
        extends AbstractTestFunctions
{
    @BeforeClass
    public void setUp()
    {
        registerParametricScalar(CastVarcharToInteger.class);
        registerParametricScalar(CastAnyToVarchar.class);
        registerParametricScalar(CastAnyFromVarchar.class);
    }

    @Test
    public void testConventionDependencies()
    {
        assertFunction("cast_varchar_to_integer('11')", INTEGER, 11);
        assertFunction("cast_any_to_varchar(BIGINT '11')", VARCHAR, "11");
        assertFunction("cast_any_to_varchar(DATE '2005-05-05')", VARCHAR, "2005-05-05");
        assertFunction("cast_any_from_varchar(DATE '2005-05-05', '2005-05-05')", BOOLEAN, true);
        assertFunction("cast_any_from_varchar(BIGINT '11', '12')", BOOLEAN, false);
    }

    @ScalarFunction("cast_varchar_to_integer")
    public static class CastVarcharToInteger
    {
        @SqlType(StandardTypes.INTEGER)
        public static long castVarcharToInteger(
                @CastDependency(fromType = StandardTypes.VARCHAR, toType = StandardTypes.INTEGER) MethodHandle cast,
                @SqlType(StandardTypes.VARCHAR) Slice value)
        {
            try {
                return (long) cast.invokeExact(value);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    @ScalarFunction("cast_any_to_varchar")
    public static class CastAnyToVarchar
    {
        @TypeParameter("V")
        @SqlType(StandardTypes.VARCHAR)
        public static Slice castAnyToVarchar(
                @CastDependency(fromType = "V", toType = StandardTypes.VARCHAR) MethodHandle cast,
                @SqlType("V") long value)
        {
            try {
                return (Slice) cast.invokeExact(value);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }

    @ScalarFunction("cast_any_from_varchar")
    public static class CastAnyFromVarchar
    {
        @TypeParameter("V")
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean castAnyFromVarchar(
                @CastDependency(fromType = StandardTypes.VARCHAR, toType = "V") MethodHandle cast,
                @OperatorDependency(operator = EQUAL, argumentTypes = {"V", "V"}) MethodHandle equals,
                @SqlType("V") long left,
                @SqlType(StandardTypes.VARCHAR) Slice right)
        {
            try {
                long rightLong = (long) cast.invokeExact(right);
                return (Boolean) equals.invokeExact(left, rightLong);
            }
            catch (Throwable t) {
                throwIfInstanceOf(t, Error.class);
                throwIfInstanceOf(t, PrestoException.class);
                throw new PrestoException(GENERIC_INTERNAL_ERROR, t);
            }
        }
    }
}
