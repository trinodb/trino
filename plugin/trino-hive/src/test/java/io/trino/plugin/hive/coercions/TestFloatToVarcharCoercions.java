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
import io.trino.plugin.hive.HiveStorageFormat;
import io.trino.plugin.hive.coercions.CoercionUtils.CoercionContext;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.HiveStorageFormat.ORC;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static java.lang.Float.floatToIntBits;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestFloatToVarcharCoercions
{
    @Test
    public void testFloatToVarcharCoercions()
    {
        testFloatToVarcharCoercions(Float.NEGATIVE_INFINITY, true);
        testFloatToVarcharCoercions(Float.MIN_VALUE, true);
        testFloatToVarcharCoercions(Float.MAX_VALUE, true);
        testFloatToVarcharCoercions(Float.POSITIVE_INFINITY, true);
        testFloatToVarcharCoercions(Float.parseFloat("123456789.12345678"), true);

        testFloatToVarcharCoercions(Float.NEGATIVE_INFINITY, false);
        testFloatToVarcharCoercions(Float.MIN_VALUE, false);
        testFloatToVarcharCoercions(Float.MAX_VALUE, false);
        testFloatToVarcharCoercions(Float.POSITIVE_INFINITY, false);
        testFloatToVarcharCoercions(Float.parseFloat("123456789.12345678"), false);
    }

    private void testFloatToVarcharCoercions(Float floatValue, boolean treatNaNAsNull)
    {
        assertCoercions(REAL, floatValue, createUnboundedVarcharType(), Slices.utf8Slice(floatValue.toString()), treatNaNAsNull ? ORC : PARQUET);
    }

    @Test
    public void testFloatToSmallerVarcharCoercions()
    {
        testFloatToSmallerVarcharCoercions(Float.NEGATIVE_INFINITY, true);
        testFloatToSmallerVarcharCoercions(Float.MIN_VALUE, true);
        testFloatToSmallerVarcharCoercions(Float.MAX_VALUE, true);
        testFloatToSmallerVarcharCoercions(Float.POSITIVE_INFINITY, true);
        testFloatToSmallerVarcharCoercions(Float.parseFloat("123456789.12345678"), true);

        testFloatToSmallerVarcharCoercions(Float.NEGATIVE_INFINITY, false);
        testFloatToSmallerVarcharCoercions(Float.MIN_VALUE, false);
        testFloatToSmallerVarcharCoercions(Float.MAX_VALUE, false);
        testFloatToSmallerVarcharCoercions(Float.POSITIVE_INFINITY, false);
        testFloatToSmallerVarcharCoercions(Float.parseFloat("123456789.12345678"), false);
    }

    private void testFloatToSmallerVarcharCoercions(Float floatValue, boolean treatNaNAsNull)
    {
        assertThatThrownBy(() -> assertCoercions(REAL, floatValue, createVarcharType(1), floatValue.toString(), treatNaNAsNull ? ORC : PARQUET))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of %s exceeds varchar(1) bounds", floatValue);
    }

    @Test
    public void testNaNToVarcharCoercions()
    {
        assertCoercions(REAL, Float.NaN, createUnboundedVarcharType(), null, ORC);

        assertCoercions(REAL, Float.NaN, createUnboundedVarcharType(), Slices.utf8Slice("NaN"), PARQUET);
        assertThatThrownBy(() -> assertCoercions(REAL, Float.NaN, createVarcharType(1), "NaN", PARQUET))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of NaN exceeds varchar(1) bounds");
    }

    public static void assertCoercions(Type fromType, Float valueToBeCoerced, Type toType, Object expectedValue, HiveStorageFormat storageFormat)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, storageFormat)).orElseThrow()
                .apply(nativeValueToBlock(fromType, (long) floatToIntBits(valueToBeCoerced)));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
