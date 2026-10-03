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
package io.trino.plugin.functions.python;

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceInput;
import io.trino.spi.block.Block;
import io.trino.spi.block.SqlMap;
import io.trino.spi.block.SqlRow;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.type.IntervalYearMonthType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.functions.python.TrinoTypes.binaryToJava;
import static io.trino.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.TypeUtils.readNativeValue;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static io.trino.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

class TestTrinoTypes
{
    @Test
    void testIntervalResultRange()
    {
        for (long millis : new long[] {86_400_000, -86_400_000, Long.MAX_VALUE / 1000, Long.MIN_VALUE / 1000}) {
            assertThat(binaryToJava(INTERVAL_DAY_TIME, intervalResult(INTERVAL_DAY_TIME, millis))).isEqualTo(millis * 1000);
        }
        for (Type type : nestedTypes(INTERVAL_DAY_TIME)) {
            for (long millis : new long[] {Long.MAX_VALUE / 1000 + 1, Long.MIN_VALUE / 1000 - 1, 9_223_372_108_800_000L, -9_223_372_108_800_000L}) {
                assertTrinoExceptionThrownBy(() -> binaryToJava(type, intervalResult(type, millis)))
                        .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                        .hasMessage("Python function returned an interval outside the supported range");
            }
        }
    }

    @Test
    void testIntervalResultNormalization()
    {
        for (Object[] example : new Object[][] {
                {"interval day(2)", 86_399_999L, 0L},
                {"interval day(2)", -86_399_999L, 0L},
                {"interval day(2)", 86_400_001L, 86_400_000_000L},
                {"interval day(2)", -86_400_001L, -86_400_000_000L},
                {"interval second(2,0)", 499L, 0L},
                {"interval second(2,0)", 500L, 1_000_000L},
                {"interval second(2,0)", -500L, 0L},
                {"interval second(2,0)", -501L, -1_000_000L},
                {"interval second(2,1)", 150L, 200_000L},
                {"interval second(2,1)", -150L, -100_000L},
                {"interval second(2,2)", 125L, 130_000L},
                {"interval second(2,2)", -125L, -120_000L},
                {"interval second(2,6)", 500L, 500_000L},
                {"interval year(2)", 13L, 12L},
                {"interval year(2)", -13L, -12L},
                {"interval month(2)", 99L, 99L},
        }) {
            Type intervalType = TESTING_TYPE_MANAGER.fromSqlType((String) example[0]);
            for (Type type : nestedTypes(intervalType)) {
                Object result = binaryToJava(type, intervalResult(type, (long) example[1]));
                assertThat(intervalValue(type, result)).isEqualTo(example[2]);
            }
        }
    }

    @Test
    void testIntervalResultLeadingPrecision()
    {
        for (Object[] example : new Object[][] {
                {"interval second(2,6)", 100_000L},
                {"interval second(2,6)", -100_000L},
                {"interval second(2,0)", 99_500L},
                {"interval second(2,0)", -99_501L},
                {"interval second(13,0)", Long.MAX_VALUE / 1000},
                {"interval second(13,0)", Long.MIN_VALUE / 1000},
                {"interval day(2)", 8_640_000_000L},
                {"interval year(2)", 1200L},
                {"interval year(2)", -1200L},
                {"interval month(2)", 100L},
                {"interval month(2)", -100L},
        }) {
            Type intervalType = TESTING_TYPE_MANAGER.fromSqlType((String) example[0]);
            for (Type type : nestedTypes(intervalType)) {
                assertTrinoExceptionThrownBy(() -> binaryToJava(type, intervalResult(type, (long) example[1])))
                        .hasErrorCode(FUNCTION_IMPLEMENTATION_ERROR)
                        .hasMessage("Function result cannot be converted to " + intervalType.getDisplayName());
            }
        }
    }

    private static List<Type> nestedTypes(Type type)
    {
        return List.of(
                type,
                new ArrayType(type),
                RowType.anonymous(List.of(type)),
                new MapType(BIGINT, type, new TypeOperators()),
                new MapType(type, BIGINT, new TypeOperators()));
    }

    private static Object intervalValue(Type type, Object value)
    {
        return switch (type) {
            case ArrayType array -> readNativeValue(array.getElementType(), (Block) value, 0);
            case RowType row -> {
                SqlRow sqlRow = (SqlRow) value;
                yield readNativeValue(row.getFields().getFirst().getType(), sqlRow.getRawFieldBlock(0), sqlRow.getRawIndex());
            }
            case MapType map -> {
                SqlMap sqlMap = (SqlMap) value;
                yield map.getKeyType().equals(BIGINT)
                        ? readNativeValue(map.getValueType(), sqlMap.getUnderlyingValueBlock(), sqlMap.getUnderlyingValuePosition(0))
                        : readNativeValue(map.getKeyType(), sqlMap.getUnderlyingKeyBlock(), sqlMap.getUnderlyingKeyPosition(0));
            }
            default -> value;
        };
    }

    private static SliceInput intervalResult(Type type, long value)
    {
        DynamicSliceOutput output = new DynamicSliceOutput(32);
        writeIntervalResult(type, value, output);
        return output.slice().getInput();
    }

    private static void writeIntervalResult(Type type, long value, DynamicSliceOutput output)
    {
        output.writeBoolean(true);
        switch (type) {
            case ArrayType array -> {
                output.writeInt(1);
                writeIntervalResult(array.getElementType(), value, output);
            }
            case RowType row -> writeIntervalResult(row.getFields().getFirst().getType(), value, output);
            case MapType map -> {
                output.writeInt(1);
                writeIntervalResult(map.getKeyType(), value, output);
                writeIntervalResult(map.getValueType(), value, output);
            }
            case IntervalYearMonthType _ -> output.writeInt(toIntExact(value));
            default -> output.writeLong(type.equals(BIGINT) ? 1 : value);
        }
    }
}
