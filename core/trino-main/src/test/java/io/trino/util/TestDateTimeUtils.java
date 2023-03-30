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
package io.trino.util;

import org.testng.annotations.Test;

import java.time.DateTimeException;

import static io.trino.util.DateTimeUtils.parseIfIso8601DateFormat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestDateTimeUtils
{
    @Test
    @SuppressWarnings("OptionalGetWithoutIsPresent")
    public void testParseIfIso8601DateFormat()
    {
        // valid dates
        assertEquals(0, parseIfIso8601DateFormat("1970-01-01").getAsInt(), "1970-01-01");
        assertEquals(31, parseIfIso8601DateFormat("1970-02-01").getAsInt(), "1970-02-01");
        assertEquals(-31, parseIfIso8601DateFormat("1969-12-01").getAsInt(), "1969-12-01");
        assertEquals(19051, parseIfIso8601DateFormat("2022-02-28").getAsInt(), "2022-02-28");
        assertEquals(-719528, parseIfIso8601DateFormat("0000-01-01").getAsInt(), "0000-01-01");
        assertEquals(2932896, parseIfIso8601DateFormat("9999-12-31").getAsInt(), "9999-12-31");

        // format invalid
        // invalid length
        assertThat(parseIfIso8601DateFormat("1970-2-01")).isEmpty();
        // invalid year0
        assertThat(parseIfIso8601DateFormat("a970-02-10")).isEmpty();
        // invalid year1
        assertThat(parseIfIso8601DateFormat("1p70-02-10")).isEmpty();
        // invalid year2
        assertThat(parseIfIso8601DateFormat("19%0-02-10")).isEmpty();
        // invalid year3
        assertThat(parseIfIso8601DateFormat("197o-02-10")).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat("1970_02-01")).isEmpty();
        // invalid month0
        assertThat(parseIfIso8601DateFormat("1970- 2-01")).isEmpty();
        // invalid month1
        assertThat(parseIfIso8601DateFormat("1970-3.-01")).isEmpty();
        // invalid dash0
        assertThat(parseIfIso8601DateFormat("1970-02/01")).isEmpty();
        // invalid day0
        assertThat(parseIfIso8601DateFormat("1970-02-/1")).isEmpty();
        // invalid day1
        assertThat(parseIfIso8601DateFormat("1970-12-0l")).isEmpty();

        assertThat(parseIfIso8601DateFormat("1970/02/01")).isEmpty();
        assertThat(parseIfIso8601DateFormat("Dec 24 2022")).isEmpty();

        // format ok, but illegal value
        assertThatThrownBy(() -> parseIfIso8601DateFormat("2022-02-29"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid date 'February 29' as '2022' is not a leap year");
        assertThatThrownBy(() -> parseIfIso8601DateFormat("1970-32-01"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for MonthOfYear (valid values 1 - 12): 32");
        assertThatThrownBy(() -> parseIfIso8601DateFormat("1970-02-41"))
                .isInstanceOf(DateTimeException.class)
                .hasMessage("Invalid value for DayOfMonth (valid values 1 - 28/31): 41");
    }
}
