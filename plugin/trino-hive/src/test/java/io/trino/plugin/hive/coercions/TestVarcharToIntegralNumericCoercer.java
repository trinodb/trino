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
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.block.TestingSession.SESSION;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarcharToIntegralNumericCoercer
{
    @Test
    public void testVarcharToTinyintCoercions()
    {
        assertVarcharToIntegralCoercion("0", TINYINT, (byte) 0);
        assertVarcharToIntegralCoercion("-0", TINYINT, (byte) 0);
        assertVarcharToIntegralCoercion("-1", TINYINT, (byte) -1);
        assertVarcharToIntegralCoercion("+1", TINYINT, (byte) 1);
        assertVarcharToIntegralCoercion("127", TINYINT, (byte) 127);
        assertVarcharToIntegralCoercion("-128", TINYINT, (byte) -128);
        assertVarcharToIntegralCoercion("128", TINYINT, (byte) 128); // Greater than Byte.MAX_VALUE
        assertVarcharToIntegralCoercion("-129", TINYINT, (byte) -129); // Lesser than Byte.MIN_VALUE
        assertVarcharToIntegralCoercion("2147483647", TINYINT, (byte) 2147483647);
        assertVarcharToIntegralCoercion("2147483648", TINYINT, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercion("-2147483648", TINYINT, (byte) 0);
        assertVarcharToIntegralCoercion("-2147483649", TINYINT, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercion("String", TINYINT, null);
        assertVarcharToIntegralCoercion("1s", TINYINT, null);
        assertVarcharToIntegralCoercion("1e+0", TINYINT, null);
    }

    @Test
    public void testVarcharToTinyintCoercionsForOrc()
    {
        assertVarcharToIntegralCoercionForOrc("0", TINYINT, (byte) 0);
        assertVarcharToIntegralCoercionForOrc("-0", TINYINT, (byte) 0);
        assertVarcharToIntegralCoercionForOrc("-1", TINYINT, (byte) -1);
        assertVarcharToIntegralCoercionForOrc("+1", TINYINT, (byte) 1);
        assertVarcharToIntegralCoercionForOrc("127", TINYINT, (byte) 127);
        assertVarcharToIntegralCoercionForOrc("-128", TINYINT, (byte) -128);
        assertVarcharToIntegralCoercionForOrc("128", TINYINT, null); // Greater than Byte.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-129", TINYINT, null); // Lesser than Byte.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("2147483647", TINYINT, null);
        assertVarcharToIntegralCoercionForOrc("2147483648", TINYINT, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-2147483648", TINYINT, null);
        assertVarcharToIntegralCoercionForOrc("-2147483649", TINYINT, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("String", TINYINT, null);
        assertVarcharToIntegralCoercionForOrc("1s", TINYINT, null);
        assertVarcharToIntegralCoercionForOrc("1e+0", TINYINT, null);
    }

    @Test
    public void testVarcharToSmallintCoercions()
    {
        assertVarcharToIntegralCoercion("0", SMALLINT, (short) 0);
        assertVarcharToIntegralCoercion("-0", SMALLINT, (short) 0);
        assertVarcharToIntegralCoercion("-1", SMALLINT, (short) -1);
        assertVarcharToIntegralCoercion("+1", SMALLINT, (short) 1);
        assertVarcharToIntegralCoercion("32767", SMALLINT, (short) 32767);
        assertVarcharToIntegralCoercion("-32767", SMALLINT, (short) -32767);
        assertVarcharToIntegralCoercion("32768", SMALLINT, (short) 32768); // Greater than Short.MAX_VALUE
        assertVarcharToIntegralCoercion("-32769", SMALLINT, (short) -32769); // Lesser than Short.MIN_VALUE
        assertVarcharToIntegralCoercion("2147483647", SMALLINT, (short) 2147483647);
        assertVarcharToIntegralCoercion("2147483648", SMALLINT, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercion("-2147483648", SMALLINT, (short) 0);
        assertVarcharToIntegralCoercion("-2147483649", SMALLINT, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercion("String", SMALLINT, null);
        assertVarcharToIntegralCoercion("1s", SMALLINT, null);
        assertVarcharToIntegralCoercion("1e+0", SMALLINT, null);
    }

    @Test
    public void testVarcharToSmallintCoercionsForOrc()
    {
        assertVarcharToIntegralCoercionForOrc("0", SMALLINT, (short) 0);
        assertVarcharToIntegralCoercionForOrc("-0", SMALLINT, (short) 0);
        assertVarcharToIntegralCoercionForOrc("-1", SMALLINT, (short) -1);
        assertVarcharToIntegralCoercionForOrc("+1", SMALLINT, (short) 1);
        assertVarcharToIntegralCoercionForOrc("32767", SMALLINT, (short) 32767);
        assertVarcharToIntegralCoercionForOrc("-32767", SMALLINT, (short) -32767);
        assertVarcharToIntegralCoercionForOrc("32768", SMALLINT, null); // Greater than Short.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-32769", SMALLINT, null); // Lesser than Short.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("2147483647", SMALLINT, null);
        assertVarcharToIntegralCoercionForOrc("2147483648", SMALLINT, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-2147483648", SMALLINT, null);
        assertVarcharToIntegralCoercionForOrc("-2147483649", SMALLINT, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("String", SMALLINT, null);
        assertVarcharToIntegralCoercionForOrc("1s", SMALLINT, null);
        assertVarcharToIntegralCoercionForOrc("1e+0", SMALLINT, null);
    }

    @Test
    public void testVarcharToIntegerCoercions()
    {
        assertVarcharToIntegralCoercion("0", INTEGER, 0);
        assertVarcharToIntegralCoercion("-0", INTEGER, 0);
        assertVarcharToIntegralCoercion("-1", INTEGER, -1);
        assertVarcharToIntegralCoercion("+1", INTEGER, 1);
        assertVarcharToIntegralCoercion("2147483647", INTEGER, 2147483647);
        assertVarcharToIntegralCoercion("2147483648", INTEGER, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercion("-2147483648", INTEGER, -2147483648);
        assertVarcharToIntegralCoercion("-2147483649", INTEGER, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercion("String", INTEGER, null);
        assertVarcharToIntegralCoercion("1s", INTEGER, null);
        assertVarcharToIntegralCoercion("1e+0", INTEGER, null);
    }

    @Test
    public void testVarcharToIntegerCoercionsForOrc()
    {
        assertVarcharToIntegralCoercionForOrc("0", INTEGER, 0);
        assertVarcharToIntegralCoercionForOrc("-0", INTEGER, 0);
        assertVarcharToIntegralCoercionForOrc("-1", INTEGER, -1);
        assertVarcharToIntegralCoercionForOrc("+1", INTEGER, 1);
        assertVarcharToIntegralCoercionForOrc("2147483647", INTEGER, 2147483647);
        assertVarcharToIntegralCoercionForOrc("2147483648", INTEGER, null); // Greater than Integer.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-2147483648", INTEGER, -2147483648);
        assertVarcharToIntegralCoercionForOrc("-2147483649", INTEGER, null); // Lesser than Integer.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("String", INTEGER, null);
        assertVarcharToIntegralCoercionForOrc("1s", INTEGER, null);
        assertVarcharToIntegralCoercionForOrc("1e+0", INTEGER, null);
    }

    @Test
    public void testVarcharToBigintCoercions()
    {
        assertVarcharToIntegralCoercion("0", BIGINT, 0L);
        assertVarcharToIntegralCoercion("-0", BIGINT, 0L);
        assertVarcharToIntegralCoercion("-1", BIGINT, -1L);
        assertVarcharToIntegralCoercion("+1", BIGINT, 1L);
        assertVarcharToIntegralCoercion("9223372036854775807", BIGINT, 9223372036854775807L);
        assertVarcharToIntegralCoercion("-9223372036854775808", BIGINT, -9223372036854775808L);
        assertVarcharToIntegralCoercion("9223372036854775808", BIGINT, null); // Greater than Long.MAX_VALUE
        assertVarcharToIntegralCoercion("-9223372036854775809", BIGINT, null); // Lesser than Long.MIN_VALUE
        assertVarcharToIntegralCoercion("String", BIGINT, null);
        assertVarcharToIntegralCoercion("1s", BIGINT, null);
        assertVarcharToIntegralCoercion("1e+0", BIGINT, null);
    }

    @Test
    public void testVarcharToBigintCoercionsForOrc()
    {
        assertVarcharToIntegralCoercionForOrc("0", BIGINT, 0L);
        assertVarcharToIntegralCoercionForOrc("-0", BIGINT, 0L);
        assertVarcharToIntegralCoercionForOrc("-1", BIGINT, -1L);
        assertVarcharToIntegralCoercionForOrc("+1", BIGINT, 1L);
        assertVarcharToIntegralCoercionForOrc("9223372036854775807", BIGINT, 9223372036854775807L);
        assertVarcharToIntegralCoercionForOrc("-9223372036854775808", BIGINT, -9223372036854775808L);
        assertVarcharToIntegralCoercionForOrc("9223372036854775808", BIGINT, null); // Greater than Long.MAX_VALUE
        assertVarcharToIntegralCoercionForOrc("-9223372036854775809", BIGINT, null); // Lesser than Long.MIN_VALUE
        assertVarcharToIntegralCoercionForOrc("String", BIGINT, null);
        assertVarcharToIntegralCoercionForOrc("1s", BIGINT, null);
        assertVarcharToIntegralCoercionForOrc("1e+0", BIGINT, null);
    }

    private static void assertVarcharToIntegralCoercion(String actualValue, Type expectedType, Object expectedValue)
    {
        assertVarcharToIntegralCoercion(actualValue, false, expectedType, expectedValue);
    }

    private static void assertVarcharToIntegralCoercionForOrc(String actualValue, Type expectedType, Object expectedValue)
    {
        assertVarcharToIntegralCoercion(actualValue, true, expectedType, expectedValue);
    }

    private static void assertVarcharToIntegralCoercion(String actualValue, boolean isOrcFile, Type expectedType, Object expectedValue)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(createUnboundedVarcharType()), toHiveType(expectedType), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(createUnboundedVarcharType(), utf8Slice(actualValue)));
        Object coercedValue = coercedBlock.isNull(0) ? null : expectedType.getObjectValue(SESSION, coercedBlock, 0);
        assertThat(coercedValue).isEqualTo(expectedValue);
    }
}
