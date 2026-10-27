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
package io.trino.jdbc;

import io.trino.client.ClientTypeSignature;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.GregorianCalendar;
import java.util.List;

import static io.trino.jdbc.AbstractTrinoResultSet.DEFAULT_OBJECT_REPRESENTATION;
import static io.trino.jdbc.AbstractTrinoResultSet.TYPE_CONVERSIONS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Fail.fail;

public class TestAbstractTrinoResultSet
{
    @Test
    public void testDateConversion()
            throws Exception
    {
        ClientTypeSignature type = new ClientTypeSignature("date");
        for (String value : List.of("1970-01-01", "2024-02-29", "12345-01-23", "+12345-01-23")) {
            LocalDate date = LocalDate.parse(value.startsWith("12345") ? "+" + value : value);
            assertThat(TYPE_CONVERSIONS.convert(type, value, Date.class).getTime())
                    .isEqualTo(date.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli());
        }
        for (String value : List.of("0001-01-01", "1582-10-04", "1582-10-10", "1582-10-15", "1899-12-31")) {
            LocalDate date = LocalDate.parse(value);
            GregorianCalendar calendar = new GregorianCalendar(date.getYear(), date.getMonthValue() - 1, date.getDayOfMonth());
            assertThat(TYPE_CONVERSIONS.convert(type, value, Date.class).getTime())
                    .isEqualTo(calendar.getTimeInMillis());
        }
        assertThatThrownBy(() -> TYPE_CONVERSIONS.convert(type, "2023-02-29", Date.class))
                .isInstanceOf(DateTimeException.class);
    }

    @Test
    public void testRepresentationImplemented()
    {
        DEFAULT_OBJECT_REPRESENTATION.forEach((sourceRawType, target) -> {
            if (target == Object.class) {
                return;
            }
            if (!TYPE_CONVERSIONS.hasConversion(sourceRawType, target)) {
                fail(String.format("No conversion registered from %s to %s", sourceRawType, target));
            }
        });
    }
}
