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
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.hive.HiveTimestampPrecision.DEFAULT_PRECISION;
import static io.trino.plugin.hive.HiveType.toHiveType;
import static io.trino.plugin.hive.coercions.CoercionUtils.createCoercer;
import static io.trino.spi.predicate.Utils.blockToNativeValue;
import static io.trino.spi.predicate.Utils.nativeValueToBlock;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateCoercer
{
    @Test
    public void testValidVarcharToDate()
    {
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "+10000-04-13");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "1900-01-01");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "2000-01-01");
        assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-03-12");
    }

    @Test
    public void testThrowsExceptionWhenStringIsNotAValidDate()
    {
        // hive would return 2023-02-09
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-01-40", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");

        // hive would return 2024-03-13
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "2023-15-13", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");

        // hive would return null
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "invalidDate", null))
                .hasMessageMatching(".*Invalid date value.*is not a valid date.*");
    }

    @Test
    public void testThrowsExceptionWhenDateIsTooOld()
    {
        assertThatThrownBy(() -> assertVarcharToDateCoercion(createUnboundedVarcharType(), "1899-12-31", null))
                .hasMessageMatching(".*Coercion on historical dates is not supported.*");
    }

    @Test
    public void testDateToVarchar()
    {
        assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("2023-01-10"), "2023-01-10");
        assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("+10000-04-25"), "+10000-04-25");
    }

    @Test
    public void testDateToLowerBoundedVarchar()
    {
        assertThatThrownBy(() -> assertDateToVarcharCoercion(createVarcharType(8), LocalDate.parse("2023-10-23"), "2023-10-23"))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Varchar representation of '2023-10-23' exceeds varchar(8) bounds");
    }

    @Test
    public void testHistoricalDateToVarchar()
    {
        assertThatThrownBy(() -> assertDateToVarcharCoercion(createUnboundedVarcharType(), LocalDate.parse("1899-12-31"), null))
                .hasMessageMatching(".*Coercion on historical dates is not supported.*");
    }

    private void assertVarcharToDateCoercion(Type fromType, String date)
    {
        assertVarcharToDateCoercion(fromType, date, fromDateToEpochDate(date));
    }

    private void assertVarcharToDateCoercion(Type fromType, String date, Long expected)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(fromType), toHiveType(DATE), new CoercionContext(DEFAULT_PRECISION, false)).orElseThrow()
                .apply(nativeValueToBlock(fromType, utf8Slice(date)));
        assertThat(blockToNativeValue(DATE, coercedValue))
                .isEqualTo(expected);
    }

    private void assertDateToVarcharCoercion(Type toType, LocalDate date, String expected)
    {
        Block coercedValue = createCoercer(TESTING_TYPE_MANAGER, toHiveType(DATE), toHiveType(toType), new CoercionContext(DEFAULT_PRECISION, false)).orElseThrow()
                .apply(nativeValueToBlock(DATE, date.toEpochDay()));
        assertThat(blockToNativeValue(VARCHAR, coercedValue))
                .isEqualTo(utf8Slice(expected));
    }

    private long fromDateToEpochDate(String dateString)
    {
        LocalDate date = LocalDate.parse(dateString);
        return date.toEpochDay();
    }
}
