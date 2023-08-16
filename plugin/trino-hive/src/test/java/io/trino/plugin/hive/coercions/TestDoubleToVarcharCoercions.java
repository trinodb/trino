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

import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDoubleToVarcharCoercions
{
    @Test(dataProvider = "doubleValues")
    public void testNaNToVarcharCoercions(Double doubleValue)
    {
        assertCoercions(DOUBLE, doubleValue, createUnboundedVarcharType(), Slices.utf8Slice(doubleValue.toString()));
    }

    @Test(dataProvider = "doubleValues")
    public void testDoubleSmallerVarcharCoercions(Double doubleValue)
    {
        assertThatThrownBy(() -> assertCoercions(DOUBLE, doubleValue, createVarcharType(1), doubleValue.toString()))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of %s exceeds varchar(1) bounds", doubleValue);
    }

    @DataProvider
    public Object[][] doubleValues()
    {
        return new Object[][] {
                {Double.MAX_VALUE},
                {Double.MAX_VALUE},
                {Double.parseDouble("123456789.12345678")},
                {Double.NaN},
        };
    }

    public static void assertCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), DEFAULT_PRECISION).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
