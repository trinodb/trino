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

    private void assertBooleanToVarcharCoercion(Type toType, boolean valueToBeCoerced, Slice expectedValue)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(BOOLEAN), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, false)).orElseThrow()
                .apply(nativeValueToBlock(BOOLEAN, valueToBeCoerced));
        assertThat(blockToNativeValue(toType, coercedValue))
                .isEqualTo(expectedValue);
    }
}
