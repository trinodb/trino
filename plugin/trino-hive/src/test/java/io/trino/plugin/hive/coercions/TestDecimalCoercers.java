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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BigintType.BIGINT;
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
        assertDecimalToIntCoercion(parseResult.getType(), parseResult.getObject(), coercedType, expectedValue);
    }

    private void assertDecimalToIntCoercion(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionUtils.CoercionContext(NANOSECONDS, false)).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
