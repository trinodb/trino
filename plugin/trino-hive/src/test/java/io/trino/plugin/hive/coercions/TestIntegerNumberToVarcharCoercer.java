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

import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.block.TestingSession.SESSION;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIntegerNumberToVarcharCoercer
{
    @Test
    public void testTinyintToVarcharCoercions()
    {
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, VARCHAR);
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, createVarcharType(1));
        assertIntegerNumberToVarcharCoercion(TINYINT, 0, VARCHAR, "0");
        assertIntegerNumberToVarcharCoercion(TINYINT, 0, createVarcharType(1), "0");
        assertIntegerNumberToVarcharCoercion(TINYINT, -1, VARCHAR, "-1");
        assertIntegerNumberToVarcharCoercion(TINYINT, -1, createVarcharType(2), "-1");
        assertIntegerNumberToVarcharCoercion(TINYINT, 1, VARCHAR, "1");
        assertIntegerNumberToVarcharCoercion(TINYINT, 1, createVarcharType(1), "1");
        assertIntegerNumberToVarcharCoercion(TINYINT, Byte.MAX_VALUE, VARCHAR, "127");
        assertIntegerNumberToVarcharCoercion(TINYINT, Byte.MAX_VALUE, createVarcharType(3), "127");
        assertIntegerNumberToVarcharCoercion(TINYINT, Byte.MIN_VALUE, VARCHAR, "-128");
        assertIntegerNumberToVarcharCoercion(TINYINT, Byte.MIN_VALUE, createVarcharType(4), "-128");
    }

    @Test
    public void testSmallintToVarcharCoercions()
    {
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, VARCHAR);
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, createVarcharType(1));
        assertIntegerNumberToVarcharCoercion(SMALLINT, 0, VARCHAR, "0");
        assertIntegerNumberToVarcharCoercion(SMALLINT, 0, createVarcharType(1), "0");
        assertIntegerNumberToVarcharCoercion(SMALLINT, -129, VARCHAR, "-129");
        assertIntegerNumberToVarcharCoercion(SMALLINT, -129, createVarcharType(4), "-129");
        assertIntegerNumberToVarcharCoercion(SMALLINT, 128, VARCHAR, "128");
        assertIntegerNumberToVarcharCoercion(SMALLINT, 128, createVarcharType(3), "128");
        assertIntegerNumberToVarcharCoercion(SMALLINT, Short.MAX_VALUE, VARCHAR, "32767");
        assertIntegerNumberToVarcharCoercion(SMALLINT, Short.MAX_VALUE, createVarcharType(5), "32767");
        assertIntegerNumberToVarcharCoercion(SMALLINT, Short.MIN_VALUE, VARCHAR, "-32768");
        assertIntegerNumberToVarcharCoercion(SMALLINT, Short.MIN_VALUE, createVarcharType(6), "-32768");
    }

    @Test
    public void testIntegerToVarcharCoercions()
    {
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, VARCHAR);
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, createVarcharType(1));
        assertIntegerNumberToVarcharCoercion(INTEGER, 0, VARCHAR, "0");
        assertIntegerNumberToVarcharCoercion(INTEGER, 0, createVarcharType(1), "0");
        assertIntegerNumberToVarcharCoercion(INTEGER, -32768, VARCHAR, "-32768");
        assertIntegerNumberToVarcharCoercion(INTEGER, -32769, createVarcharType(6), "-32769");
        assertIntegerNumberToVarcharCoercion(INTEGER, 32768, VARCHAR, "32768");
        assertIntegerNumberToVarcharCoercion(INTEGER, 32768, createVarcharType(5), "32768");
        assertIntegerNumberToVarcharCoercion(INTEGER, Integer.MAX_VALUE, VARCHAR, "2147483647");
        assertIntegerNumberToVarcharCoercion(INTEGER, Integer.MAX_VALUE, createVarcharType(10), "2147483647");
        assertIntegerNumberToVarcharCoercion(INTEGER, Integer.MIN_VALUE, VARCHAR, "-2147483648");
        assertIntegerNumberToVarcharCoercion(INTEGER, Integer.MIN_VALUE, createVarcharType(11), "-2147483648");
    }

    @Test
    public void testBigintToVarcharCoercions()
    {
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, VARCHAR);
        assertIntegerNumberToVarcharCoercionForNull(TINYINT, createVarcharType(1));
        assertIntegerNumberToVarcharCoercion(BIGINT, 0, VARCHAR, "0");
        assertIntegerNumberToVarcharCoercion(BIGINT, 0, createVarcharType(1), "0");
        assertIntegerNumberToVarcharCoercion(BIGINT, -2147483648, VARCHAR, "-2147483648");
        assertIntegerNumberToVarcharCoercion(BIGINT, -2147483649L, createVarcharType(11), "-2147483649");
        assertIntegerNumberToVarcharCoercion(BIGINT, 2147483647, VARCHAR, "2147483647");
        assertIntegerNumberToVarcharCoercion(BIGINT, 2147483648L, createVarcharType(10), "2147483648");
        assertIntegerNumberToVarcharCoercion(BIGINT, Long.MAX_VALUE, VARCHAR, "9223372036854775807");
        assertIntegerNumberToVarcharCoercion(BIGINT, Long.MAX_VALUE, createVarcharType(19), "9223372036854775807");
        assertIntegerNumberToVarcharCoercion(BIGINT, Long.MIN_VALUE, VARCHAR, "-9223372036854775808");
        assertIntegerNumberToVarcharCoercion(BIGINT, Long.MIN_VALUE, createVarcharType(20), "-9223372036854775808");
    }

    @Test
    public void testIntegerNumberToLowerBoundVarcharCoercions()
    {
        assertCoercionFailureForLowerBoundedVarchar(TINYINT, -1, createVarcharType(1));
        assertCoercionFailureForLowerBoundedVarchar(TINYINT, Byte.MAX_VALUE, createVarcharType(2));
        assertCoercionFailureForLowerBoundedVarchar(TINYINT, Byte.MIN_VALUE, createVarcharType(3));

        assertCoercionFailureForLowerBoundedVarchar(SMALLINT, -1, createVarcharType(1));
        assertCoercionFailureForLowerBoundedVarchar(SMALLINT, Short.MAX_VALUE, createVarcharType(4));
        assertCoercionFailureForLowerBoundedVarchar(SMALLINT, Short.MIN_VALUE, createVarcharType(5));

        assertCoercionFailureForLowerBoundedVarchar(INTEGER, -1, createVarcharType(1));
        assertCoercionFailureForLowerBoundedVarchar(INTEGER, Integer.MAX_VALUE, createVarcharType(9));
        assertCoercionFailureForLowerBoundedVarchar(INTEGER, Integer.MIN_VALUE, createVarcharType(10));

        assertCoercionFailureForLowerBoundedVarchar(BIGINT, -1, createVarcharType(1));
        assertCoercionFailureForLowerBoundedVarchar(BIGINT, Long.MAX_VALUE, createVarcharType(18));
        assertCoercionFailureForLowerBoundedVarchar(BIGINT, Long.MIN_VALUE, createVarcharType(19));
    }

    private static void assertIntegerNumberToVarcharCoercion(Type actualType, long actualValue, Type expectedType, String expectedValue)
    {
        assertIntegerNumberToVarcharCoercion(actualType, actualValue, true, expectedType, expectedValue);
        assertIntegerNumberToVarcharCoercion(actualType, actualValue, false, expectedType, expectedValue);
    }

    private static void assertIntegerNumberToVarcharCoercionForNull(Type actualType, Type expectedType)
    {
        assertIntegerNumberToVarcharCoercion(actualType, null, true, expectedType, null);
        assertIntegerNumberToVarcharCoercion(actualType, null, false, expectedType, null);
    }

    private static void assertCoercionFailureForLowerBoundedVarchar(Type actualType, long actualValue, Type expectedType)
    {
        assertThatThrownBy(() -> assertIntegerNumberToVarcharCoercion(actualType, actualValue, true, expectedType, String.valueOf(actualValue)))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Varchar representation of %s exceeds %s bounds".formatted(actualValue, expectedType));
    }

    private static void assertIntegerNumberToVarcharCoercion(Type actualType, Object actualValue, boolean isOrcFile, Type expectedType, String expectedValue)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(actualType), toHiveType(expectedType), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(actualType, actualValue));
        Object coercedValue = coercedBlock.isNull(0) ? null : expectedType.getObjectValue(SESSION, coercedBlock, 0);
        assertThat(coercedValue).isEqualTo(expectedValue);
    }
}
