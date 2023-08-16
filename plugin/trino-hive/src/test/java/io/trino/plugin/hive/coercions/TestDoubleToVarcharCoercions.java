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
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.stream.Stream;

import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.DataProviders.cartesianProduct;
import static io.trino.testing.DataProviders.toDataProvider;
import static io.trino.testing.DataProviders.trueFalse;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDoubleToVarcharCoercions
{
    @Test(dataProvider = "doubleValues")
    public void testDoubleToVarcharCoercions(Double doubleValue, boolean treatNaNAsNull)
    {
        assertCoercions(DOUBLE, doubleValue, createUnboundedVarcharType(), Slices.utf8Slice(doubleValue.toString()), treatNaNAsNull);
    }

    @Test(dataProvider = "doubleValues")
    public void testDoubleSmallerVarcharCoercions(Double doubleValue, boolean treatNaNAsNull)
    {
        assertThatThrownBy(() -> assertCoercions(DOUBLE, doubleValue, createVarcharType(1), doubleValue.toString(), treatNaNAsNull))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of %s exceeds varchar(1) bounds", doubleValue);
    }

    @Test
    public void testNaNToVarcharCoercions()
    {
        assertCoercions(DOUBLE, Double.NaN, createUnboundedVarcharType(), null, true);

        assertCoercions(DOUBLE, Double.NaN, createUnboundedVarcharType(), Slices.utf8Slice("NaN"), false);
        assertThatThrownBy(() -> assertCoercions(DOUBLE, Double.NaN, createVarcharType(1), "NaN", false))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of NaN exceeds varchar(1) bounds");
    }

    @DataProvider
    public Object[][] doubleValues()
    {
        return cartesianProduct(
                Stream.of(
                        Double.NEGATIVE_INFINITY,
                        Double.MIN_VALUE,
                        Double.MAX_VALUE,
                        Double.POSITIVE_INFINITY,
                        Double.parseDouble("123456789.12345678"))
                        .collect(toDataProvider()),
                trueFalse());
    }

    public static void assertCoercions(Type fromType, Object valueToBeCoerced, Type toType, Object expectedValue, boolean treatNaNAsNull)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, treatNaNAsNull)).orElseThrow()
                .apply(nativeValueToBlock(fromType, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
