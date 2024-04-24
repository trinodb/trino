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

import io.airlift.slice.Slice;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestBooleanCoercer
{
    @Test
    public void testBooleanToVarchar()
    {
        assertBooleanToVarcharCoercion(createUnboundedVarcharType(), true, utf8Slice("TRUE"));
        assertBooleanToVarcharCoercion(createUnboundedVarcharType(), false, utf8Slice("FALSE"));
    }

    @Test
    public void testBooleanToLowerBoundedVarchar()
    {
        assertThatThrownBy(() -> assertBooleanToVarcharCoercion(createVarcharType(1), true, utf8Slice("T")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of true exceeds varchar(1) bounds");
        assertThatThrownBy(() -> assertBooleanToVarcharCoercion(createVarcharType(1), false, utf8Slice("F")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of false exceeds varchar(1) bounds");
    }

    @Test
    public void testVarcharToBoolean()
    {
        // Valid false values
        assertVarcharToBooleanCoercion("FALSE", false);
        assertVarcharToBooleanCoercion("OFF", false);
        assertVarcharToBooleanCoercion("NO", false);
        assertVarcharToBooleanCoercion("0", false);
        assertVarcharToBooleanCoercion("", false);

        // false values in mixed cases
        assertVarcharToBooleanCoercion("FAlSE", false);
        assertVarcharToBooleanCoercion("OfF", false);
        assertVarcharToBooleanCoercion("nO", false);
        assertVarcharToBooleanCoercion("no", false);

        // True values
        assertVarcharToBooleanCoercion("YES", true);
        assertVarcharToBooleanCoercion("TRUE", true);
        assertVarcharToBooleanCoercion("TR", true);
        assertVarcharToBooleanCoercion("T", true);
        assertVarcharToBooleanCoercion("Y", true);
        assertVarcharToBooleanCoercion("1", true);
        assertVarcharToBooleanCoercion("-1", true);
        assertVarcharToBooleanCoercion("-123", true);
        assertVarcharToBooleanCoercion("123", true);

        // Extension of false values
        assertVarcharToBooleanCoercion("FALSEE", true);
        assertVarcharToBooleanCoercion("OFFF", true);
        assertVarcharToBooleanCoercion("NO0", true);
        assertVarcharToBooleanCoercion("00", true);
    }

    @Test
    public void testVarcharToBooleanForOrc()
    {
        // Valid false values
        assertVarcharToBooleanCoercion("0", true, false);
        assertVarcharToBooleanCoercion("-0", true, false);
        assertVarcharToBooleanCoercion("00", true, false);

        // True values
        assertVarcharToBooleanCoercion("1", true, true);
        assertVarcharToBooleanCoercion("-1", true, true);
        assertVarcharToBooleanCoercion("-123", true, true);
        assertVarcharToBooleanCoercion("123", true, true);

        // Non numeric values
        assertVarcharToBooleanCoercion("FALSE", true, null);
        assertVarcharToBooleanCoercion("OFF", true, null);
        assertVarcharToBooleanCoercion("NO", true, null);
        assertVarcharToBooleanCoercion("T", true, null);
        assertVarcharToBooleanCoercion("Y", true, null);
    }

    private void assertBooleanToVarcharCoercion(Type toType, boolean valueToBeCoerced, Slice expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(BOOLEAN), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, false)).orElseThrow()
                .apply(nativeValueToBlock(BOOLEAN, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }

    private void assertVarcharToBooleanCoercion(String valueToBeCoerced, Boolean expectedValue)
    {
        assertVarcharToBooleanCoercion(valueToBeCoerced, false, expectedValue);
    }

    private void assertVarcharToBooleanCoercion(String valueToBeCoerced, boolean isOrcFile, Boolean expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(createUnboundedVarcharType()), toHiveType(BOOLEAN), new CoercionContext(DEFAULT_PRECISION, isOrcFile)).orElseThrow()
                .apply(nativeValueToBlock(createUnboundedVarcharType(), utf8Slice(valueToBeCoerced)));
        assertThat(blockToNativeValue(BOOLEAN, coercedValue))
                .isEqualTo(expectedValue);
    }
}
