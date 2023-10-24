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
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Double.NEGATIVE_INFINITY;
import static java.lang.Double.NaN;
import static java.lang.Double.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarcharToDoubleCoercer
{
    @Test
    public void testDoubleToVarcharCoercions()
    {
        // Below infinity
        assertVarcharToDoubleCoercion("-1.7976931348623157e+310", NEGATIVE_INFINITY);
        assertVarcharToDoubleCoercion("-Infinity", NEGATIVE_INFINITY);
        assertVarcharToDoubleCoercion("1.12e+3", Double.parseDouble("1120.0"));
        assertVarcharToDoubleCoercion("123456789.12345678", Double.parseDouble("123456789.12345678"));
        assertVarcharToDoubleCoercion("123", Double.parseDouble("123"));
        assertVarcharToDoubleCoercion("Infinity", POSITIVE_INFINITY);
        assertVarcharToDoubleCoercion("+Infinity", POSITIVE_INFINITY);
        // Above infinity
        assertVarcharToDoubleCoercion("1.7976931348623157e+310", POSITIVE_INFINITY);
        // Invalid string
        assertVarcharToDoubleCoercion("Hello", null);
    }

    @Test
    public void testNaNToVarcharCoercions()
    {
        assertVarcharToDoubleCoercion("NaN", true, null);
        assertVarcharToDoubleCoercion("NaN", false, NaN);
    }

    private static void assertVarcharToDoubleCoercion(String actualValue, Double expectedValue)
    {
        assertVarcharToDoubleCoercion(actualValue, false, expectedValue);
    }

    private static void assertVarcharToDoubleCoercion(String actualValue, boolean treatNaNAsNull, Double expectedValue)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(createUnboundedVarcharType()), toHiveType(DOUBLE), new CoercionContext(DEFAULT_PRECISION, treatNaNAsNull)).orElseThrow()
                .apply(nativeValueToBlock(createUnboundedVarcharType(), utf8Slice(actualValue)));
        assertThat(blockToNativeValue(DOUBLE, coercedBlock))
                .isEqualTo(expectedValue);
    }
}
