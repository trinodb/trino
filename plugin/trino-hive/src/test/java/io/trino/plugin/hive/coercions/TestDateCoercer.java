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
import io.trino.spi.type.DateType;
import io.trino.spi.type.Type;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.time.LocalDate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.NANOSECONDS;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateCoercer
{
    @Test(dataProvider = "validDateProvider")
    public void testValidVarcharToDate(String date)
    {
        assertVarcharToDateCoercion(createUnboundedVarcharType(), date);
    }

    @DataProvider
    public static Object[][] validDateProvider()
    {
        return new Object[][] {
                {"+10000-04-13"},
                {"1900-01-01"},
                {"2000-01-01"},
                {"2023-03-12"}};
    }

    @Test(dataProvider = "invalidDateProvider")
    public void testThrowsExceptionWhenStringIsNotAValidDate(String date)
    {
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), date, null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");
    }

    @DataProvider
    public static Object[][] invalidDateProvider()
    {
        return new Object[][] {
                {"2023-01-40"}, // hive would return 2023-02-09
                {"2023-15-13"}, // hive would return 2024-03-13
                {"invalidDate"}}; // hive would return null
    }

    @Test
    public void testThrowsExceptionWhenDateIsTooOld()
    {
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "1899-12-31", null))
                .hasMessageMatching(".*Coercion on historical dates is not supported.*");
    }

    private void assertVarcharToDateCoercion(Type fromType, String date)
    {
        assertVarcharToDateCoercion(fromType, date, fromDateToEpochDate(date));
    }

    private void assertVarcharToDateCoercion(Type fromType, String date, Long expected)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(DateType.DATE), new CoercionContext(NANOSECONDS, false)).orElseThrow()
                .apply(nativeValueToBlock(fromType, utf8Slice(date)));
        assertThat(blockToNativeValue(DateType.DATE, coercedValue))
                .isEqualTo(expected);
    }

    private long fromDateToEpochDate(String dateString)
    {
        LocalDate date = LocalDate.parse(dateString);
        return date.toEpochDay();
    }
}
