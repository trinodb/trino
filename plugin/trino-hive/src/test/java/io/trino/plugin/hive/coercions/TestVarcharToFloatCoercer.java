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
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.MAX_VALUE;
import static java.lang.Float.MIN_NORMAL;
import static java.lang.Float.MIN_VALUE;
import static java.lang.Float.NEGATIVE_INFINITY;
import static java.lang.Float.NaN;
import static java.lang.Float.POSITIVE_INFINITY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestVarcharToFloatCoercer
{
    @Test
    public void testVarcharToFloatCoercions()
    {
        assertVarcharToFloatCoercion("1.12e+3", Float.parseFloat("1120.0"));
        assertVarcharToFloatCoercion("123456789.12345678", Float.parseFloat("123456789.12345678"));
        assertVarcharToFloatCoercion("123", Float.parseFloat("123"));
        assertVarcharToFloatCoercion("1.17549435E-38f", MIN_NORMAL); // Value where Float.intBitsToFloat(0x00800000)
        assertVarcharToFloatCoercion("3.4028235e+38f", MAX_VALUE); // largest positive value
        assertVarcharToFloatCoercion("-3.4028235e+38f", -3.4028235E38f); // largest negative value
        assertVarcharToFloatCoercion("4.4028235e+39f", POSITIVE_INFINITY); // Value above max positive value
        assertVarcharToFloatCoercion("-3.4028235e+39f", NEGATIVE_INFINITY); // Value below max negative value
        assertVarcharToFloatCoercion("1.4e-45f", MIN_VALUE); // smallest positive nonzero value
        assertVarcharToFloatCoercion("1.3e-46f", 0f); // Value smaller than positive nonzero value
        assertVarcharToFloatCoercion("-1.3e-46f", -0f); // Value greater than negative nonzero value

        // Infinite values
        assertVarcharToFloatCoercion("-Infinity", NEGATIVE_INFINITY);
        assertVarcharToFloatCoercion("Infinity", POSITIVE_INFINITY);
        assertVarcharToFloatCoercion("+Infinity", POSITIVE_INFINITY);

        // Valid double values
        assertVarcharToFloatCoercion("1.12e+3d", Float.parseFloat("1120.0"));
        assertVarcharToFloatCoercion(Double.toString(Double.MAX_VALUE), POSITIVE_INFINITY);
        assertVarcharToFloatCoercion(Double.toString(Double.MIN_VALUE), 0.0f);

        // Invalid string
        assertVarcharToFloatCoercion("1.13e", null);
        assertVarcharToFloatCoercion("Hello", null);
    }

    @Test
    public void testVarcharToFloatNaNCoercions()
    {
        assertVarcharToFloatCoercion("NaN", true, null);
        assertVarcharToFloatCoercion("NaN", false, NaN);
    }

    private static void assertVarcharToFloatCoercion(String actualValue, Float expectedValue)
    {
        assertVarcharToFloatCoercion(actualValue, false, expectedValue);
    }

    private static void assertVarcharToFloatCoercion(String actualValue, boolean isOrcFile, Float expectedValue)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(createUnboundedVarcharType()), toHiveType(REAL), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(createUnboundedVarcharType(), utf8Slice(actualValue)));
        Float coercedValue = coercedBlock.isNull(0) ? null : REAL.getFloat(coercedBlock, 0);
        assertThat(coercedValue).isEqualTo(expectedValue);
    }
}
