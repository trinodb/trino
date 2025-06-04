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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestIntegerNumberToDoubleCoercer
{
    @Test
    public void testTinyintToDoubleCoercion()
    {
        assertDoubleCoercion(TINYINT, 0L, 0D);
        assertDoubleCoercion(TINYINT, 12L, 12D);
        assertDoubleCoercion(TINYINT, -12L, -12D);
        assertDoubleCoercion(TINYINT, (long) Byte.MAX_VALUE, 127D);
        assertDoubleCoercion(TINYINT, (long) Byte.MIN_VALUE, -128D);
    }

    @Test
    public void testSmallintToDoubleCoercion()
    {
        assertDoubleCoercion(SMALLINT, 0L, 0D);
        assertDoubleCoercion(SMALLINT, 128L, 128D);
        assertDoubleCoercion(SMALLINT, -128L, -128D);
        assertDoubleCoercion(SMALLINT, (long) Short.MAX_VALUE, 32767D);
        assertDoubleCoercion(SMALLINT, (long) Short.MIN_VALUE, -32768D);
    }

    @Test
    public void testIntToDoubleCoercion()
    {
        assertDoubleCoercion(INTEGER, 0L, 0D);
        assertDoubleCoercion(INTEGER, 128L, 128D);
        assertDoubleCoercion(INTEGER, -128L, -128D);
        assertDoubleCoercion(INTEGER, (long) Integer.MAX_VALUE, 2147483647D);
        assertDoubleCoercion(INTEGER, (long) Integer.MIN_VALUE, -2147483648D);
    }

    @Test
    public void testBigintToDoubleCoercion()
    {
        assertDoubleCoercion(BIGINT, 0L, 0D);
        assertDoubleCoercion(BIGINT, 128L, 128D);
        assertDoubleCoercion(BIGINT, -128L, -128D);
        assertDoubleCoercion(BIGINT, (long) Integer.MAX_VALUE, 2147483647D);
        assertDoubleCoercion(BIGINT, (long) Integer.MIN_VALUE, -2147483648D);
        assertDoubleCoercion(BIGINT, 4503599627370495L, 4503599627370495D);
        assertDoubleCoercion(BIGINT, -4503599627370496L, -4503599627370496D);

        assertDoubleCoercionFailure(BIGINT, 4503599627370496L);
        assertDoubleCoercionFailure(BIGINT, -4503599627370497L);
        assertDoubleCoercionFailure(BIGINT, Long.MAX_VALUE);
        assertDoubleCoercionFailure(BIGINT, Long.MIN_VALUE);
    }

    private static void assertDoubleCoercion(Type fromType, Object valueToBeCoerced, Double expectedValue)
    {
        Block coercedValue = createIntegerToDoubleCoercer(fromType, valueToBeCoerced);
        assertThat(blockToNativeValue(DOUBLE, coercedValue))
                .isEqualTo(expectedValue);
    }

    private static void assertDoubleCoercionFailure(Type fromType, Object valueToBeCoerced)
    {
        assertThatThrownBy(() -> createIntegerToDoubleCoercer(fromType, valueToBeCoerced))
                .hasMessageContaining("Cannot read value '%s' as DOUBLE".formatted(valueToBeCoerced));
    }

    private static Block createIntegerToDoubleCoercer(Type fromType, Object valueToBeCoerced)
    {
        return createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(DOUBLE), new CoercionUtils.CoercionContext(DEFAULT_PRECISION, PARQUET)).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
    }
}
