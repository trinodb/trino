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
package io.trino.operator.scalar;

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.operator.scalar.DateTimeUnit.DAY;
import static io.trino.operator.scalar.DateTimeUnit.HOUR;
import static io.trino.operator.scalar.DateTimeUnit.MILLISECOND;
import static io.trino.operator.scalar.DateTimeUnit.MINUTE;
import static io.trino.operator.scalar.DateTimeUnit.MONTH;
import static io.trino.operator.scalar.DateTimeUnit.QUARTER;
import static io.trino.operator.scalar.DateTimeUnit.SECOND;
import static io.trino.operator.scalar.DateTimeUnit.WEEK;
import static io.trino.operator.scalar.DateTimeUnit.YEAR;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

@Test
public class TestDateTimeUnit
{
    @Test
    public void testSingleUnitForm()
    {
        assertThat(forString("millisecond")).hasValue(MILLISECOND);
        assertThat(forString("second")).hasValue(SECOND);
        assertThat(forString("minute")).hasValue(MINUTE);
        assertThat(forString("hour")).hasValue(HOUR);
        assertThat(forString("day")).hasValue(DAY);
        assertThat(forString("week")).hasValue(WEEK);
        assertThat(forString("month")).hasValue(MONTH);
        assertThat(forString("quarter")).hasValue(QUARTER);
        assertThat(forString("year")).hasValue(YEAR);
    }

    @Test
    public void testPluralUnitForm()
    {
        assertThat(forString("milliseconds")).hasValue(MILLISECOND);
        assertThat(forString("seconds")).hasValue(SECOND);
        assertThat(forString("minutes")).hasValue(MINUTE);
        assertThat(forString("hours")).hasValue(HOUR);
        assertThat(forString("days")).hasValue(DAY);
        assertThat(forString("weeks")).hasValue(WEEK);
        assertThat(forString("months")).hasValue(MONTH);
        assertThat(forString("quarters")).hasValue(QUARTER);
        assertThat(forString("years")).hasValue(YEAR);
    }

    @Test
    public void testWordCases()
    {
        assertThat(forString("Days")).hasValue(DAY);
        assertThat(forString("days")).hasValue(DAY);
        assertThat(forString("YEAR")).hasValue(YEAR);
        assertThat(forString("MoNtHs")).hasValue(MONTH);
        assertThat(forString("WeekS")).hasValue(WEEK);
    }

    @Test
    public void testUnsupportedUnits()
    {
        assertThat(forString("eons")).isEmpty();
        assertThat(forString("notAUnit")).isEmpty();
        assertThat(forString("d")).isEmpty();
        assertThat(forString("m")).isEmpty();
        assertThat(forString("y")).isEmpty();
    }

    @Test
    public void testPluralFormNotAccepted()
    {
        assertThat(forString("hours", false)).isEmpty();
        assertThat(forString("SecondS", false)).isEmpty();
        assertThat(forString("DayS", false)).isEmpty();
        assertThat(forString("Months", false)).isEmpty();
        assertThat(forString("years", false)).isEmpty();
        assertThat(forString("QUARTERS", false)).isEmpty();
    }

    public static Optional<DateTimeUnit> forString(String value)
    {
        return forString(value, true);
    }

    public static Optional<DateTimeUnit> forString(String value, boolean acceptPlural)
    {
        return DateTimeUnit.valueOf(Slices.utf8Slice(value), acceptPlural);
    }
}
