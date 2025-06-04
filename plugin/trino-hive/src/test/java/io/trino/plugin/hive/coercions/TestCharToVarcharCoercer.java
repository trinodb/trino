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
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveStorageFormat.PARQUET;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.plugin.hive.util.HiveTypeTranslator.toHiveType;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestCharToVarcharCoercer
{
    @Test
    public void testCharToSmallerVarcharCoercions()
    {
        assertCharToVarcharCoercion("a", createCharType(3), "a", createVarcharType(2));
        assertCharToVarcharCoercion(" a", createCharType(3), " a", createVarcharType(2));
        assertCharToVarcharCoercion(" aa", createCharType(4), " a", createVarcharType(2));
        assertCharToVarcharCoercion("a", createCharType(3), "a", createVarcharType(2));
        assertCharToVarcharCoercion("\uD83D\uDCB0\uD83D\uDCB0\uD83D\uDCB0", createCharType(3), "\uD83D\uDCB0", createVarcharType(1));
        assertCharToVarcharCoercion("\uD83D\uDCB0\uD83D\uDCB0", createCharType(7), "\uD83D\uDCB0\uD83D\uDCB0", createVarcharType(6));
        assertCharToVarcharCoercion("\uD83D\uDCB0", createCharType(3), "\uD83D\uDCB0", createVarcharType(1));
        assertCharToVarcharCoercion("\uD83D\uDCB0 \uD83D\uDCB0", createCharType(7), "\uD83D\uDCB0 \uD83D\uDCB0", createVarcharType(6));
    }

    private static void assertCharToVarcharCoercion(String actualValue, Type fromType, String expectedValue, Type toType)
    {
        Block coercedBlock = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, PARQUET)).orElseThrow()
                .apply(nativeValueToBlock(fromType, utf8Slice(actualValue)));
        assertThat(blockToNativeValue(toType, coercedBlock))
                .isEqualTo(utf8Slice(expectedValue));
    }
}
