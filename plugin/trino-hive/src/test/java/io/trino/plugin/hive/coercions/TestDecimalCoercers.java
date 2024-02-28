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

import io.trino.spi.block.Block;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestDecimalCoercers
{
    @Test
    public void testDecimalToIntCoercion()
    {
        testDecimalToIntCoercion("12.120000000000000000", TINYINT, 12L);
        testDecimalToIntCoercion("-12.120000000000000000", TINYINT, -12L);
        testDecimalToIntCoercion("12.120", TINYINT, 12L);
        testDecimalToIntCoercion("-12.120", TINYINT, -12L);
        testDecimalToIntCoercion("141.120000000000000000", TINYINT, null);
        testDecimalToIntCoercion("-141.120", TINYINT, null);
        testDecimalToIntCoercion("130.120000000000000000", SMALLINT, 130L);
        testDecimalToIntCoercion("-130.120000000000000000", SMALLINT, -130L);
        testDecimalToIntCoercion("130.120", SMALLINT, 130L);
        testDecimalToIntCoercion("-130.120", SMALLINT, -130L);
        testDecimalToIntCoercion("66000.30120000000000000", SMALLINT, null);
        testDecimalToIntCoercion("-66000.120", SMALLINT, null);
        testDecimalToIntCoercion("33000.12000000000000000", INTEGER, 33000L);
        testDecimalToIntCoercion("-33000.12000000000000000", INTEGER, -33000L);
        testDecimalToIntCoercion("33000.120", INTEGER, 33000L);
        testDecimalToIntCoercion("-33000.120", INTEGER, -33000L);
        testDecimalToIntCoercion("3300000000.1200000000000", INTEGER, null);
        testDecimalToIntCoercion("3300000000.120", INTEGER, null);
        testDecimalToIntCoercion("3300000000.1200000000000", BIGINT, 3300000000L);
        testDecimalToIntCoercion("-3300000000.120000000000", BIGINT, -3300000000L);
        testDecimalToIntCoercion("3300000000.12", BIGINT, 3300000000L);
        testDecimalToIntCoercion("-3300000000.12", BIGINT, -3300000000L);
        testDecimalToIntCoercion("330000000000000000000.12000000000", BIGINT, null);
        testDecimalToIntCoercion("-330000000000000000000.12000000000", BIGINT, null);
        testDecimalToIntCoercion("3300000", INTEGER, 3300000L);
    }

    private void testDecimalToIntCoercion(String decimalString, Type coercedType, Object expectedValue)
    {
        DecimalParseResult parseResult = Decimals.parse(decimalString);

        if (decimalString.length() > 19) {
            assertThat(parseResult.getType().isShort()).isFalse();
        }
        else {
            assertThat(parseResult.getType().isShort()).isTrue();
        }
        assertCoercion(parseResult.getType(), parseResult.getObject(), coercedType, expectedValue);
    }

    @Test
    public void testTinyintToDecimalCoercion()
    {
        // Short decimal coercion
        assertCoercion(TINYINT, 12L, createDecimalType(10), 12L);
        assertCoercion(TINYINT, 12L, createDecimalType(10, 2), 1_200L);
        assertCoercion(TINYINT, 12L, createDecimalType(10, 5), 1_200_000L);
        // Long decimal coercion
        assertCoercion(TINYINT, 0L, createDecimalType(), Int128.ZERO);
        assertCoercion(TINYINT, 0L, createDecimalType(), Int128.ZERO);
        assertCoercion(TINYINT, 12L, createDecimalType(), Int128.valueOf(12));
        assertCoercion(TINYINT, -12L, createDecimalType(), Int128.valueOf(-12));
        assertCoercion(TINYINT, (long) Byte.MAX_VALUE, createDecimalType(), Int128.valueOf(Byte.MAX_VALUE));
        assertCoercion(TINYINT, (long) Byte.MIN_VALUE, createDecimalType(), Int128.valueOf(Byte.MIN_VALUE));
        assertCoercion(TINYINT, 12L, createDecimalType(20, 10), Int128.valueOf("120000000000"));
        // Coercion overflow
        assertCoercion(TINYINT, 42L, createDecimalType(6, 5), null);
    }

    @Test
    public void testSmallintToDecimalCoercion()
    {
        // Short decimal coercion
        assertCoercion(SMALLINT, 12L, createDecimalType(10), 12L);
        assertCoercion(SMALLINT, 12L, createDecimalType(10, 2), 1_200L);
        assertCoercion(SMALLINT, 12L, createDecimalType(10, 5), 1_200_000L);
        // Long decimal coercion
        assertCoercion(SMALLINT, 12L, createDecimalType(20, 10), Int128.valueOf("120000000000"));
        assertCoercion(SMALLINT, 0L, createDecimalType(), Int128.ZERO);
        assertCoercion(SMALLINT, 128L, createDecimalType(), Int128.valueOf(128));
        assertCoercion(SMALLINT, -128L, createDecimalType(), Int128.valueOf(-128));
        assertCoercion(SMALLINT, (long) Short.MAX_VALUE, createDecimalType(), Int128.valueOf(Short.MAX_VALUE));
        assertCoercion(SMALLINT, (long) Short.MIN_VALUE, createDecimalType(), Int128.valueOf(Short.MIN_VALUE));
        // Coercion overflow
        assertCoercion(SMALLINT, 128L, createDecimalType(7, 5), null);
        assertCoercion(SMALLINT, 128L, createDecimalType(20, 18), null);
    }

    @Test
    public void testIntToDecimalCoercion()
    {
        // Short decimal coercion
        assertCoercion(INTEGER, 123_456L, createDecimalType(10), 123_456L);
        assertCoercion(INTEGER, 123_456L, createDecimalType(10, 3), 123_456_000L);
        // Long decimal coercion
        assertCoercion(INTEGER, 0L, createDecimalType(), Int128.ZERO);
        assertCoercion(INTEGER, 128L, createDecimalType(), Int128.valueOf(128));
        assertCoercion(INTEGER, -128L, createDecimalType(), Int128.valueOf(-128));
        assertCoercion(INTEGER, (long) Integer.MAX_VALUE, createDecimalType(), Int128.valueOf(Integer.MAX_VALUE));
        assertCoercion(INTEGER, (long) Integer.MIN_VALUE, createDecimalType(), Int128.valueOf(Integer.MIN_VALUE));
        assertCoercion(INTEGER, 123_456L, createDecimalType(20, 10), Int128.valueOf("1234560000000000"));
        // Coercion overflow
        assertCoercion(INTEGER, 123_456_789L, createDecimalType(10, 5), null);
        assertCoercion(INTEGER, 123_456_789L, createDecimalType(20, 13), null);
    }

    @Test
    public void testBigintToDecimalCoercion()
    {
        // Short decimal coercion
        assertCoercion(BIGINT, 0L, createDecimalType(10), 0L);
        assertCoercion(BIGINT, 123_456_789L, createDecimalType(12), 123_456_789L);
        assertCoercion(BIGINT, 123_456_789L, createDecimalType(12, 3), 123_456_789_000L);
        // Long decimal coercion
        assertCoercion(BIGINT, 0L, createDecimalType(), Int128.ZERO);
        assertCoercion(BIGINT, 128L, createDecimalType(), Int128.valueOf(128));
        assertCoercion(BIGINT, -128L, createDecimalType(), Int128.valueOf(-128));
        assertCoercion(BIGINT, Long.MAX_VALUE, createDecimalType(), Int128.valueOf(Long.MAX_VALUE));
        assertCoercion(BIGINT, Long.MIN_VALUE, createDecimalType(), Int128.valueOf(Long.MIN_VALUE));
        assertCoercion(BIGINT, 123_456_789L, createDecimalType(20, 5), Int128.valueOf("12345678900000"));
        assertCoercion(BIGINT, 123_456_789L, createDecimalType(20, 10), Int128.valueOf("1234567890000000000"));
        assertCoercion(BIGINT, Long.MAX_VALUE, createDecimalType(38, 2), Int128.valueOf("922337203685477580700"));
        // Coercion overflow
        assertCoercion(BIGINT, 123_456_789L, createDecimalType(10, 5), null);
        assertCoercion(BIGINT, Long.MAX_VALUE, createDecimalType(25, 8), null);
    }

    private static void assertCoercion(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionUtils.CoercionContext(DEFAULT_PRECISION, false)).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
