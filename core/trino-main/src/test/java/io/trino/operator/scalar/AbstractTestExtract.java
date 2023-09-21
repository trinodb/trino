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

import com.google.common.base.CaseFormat;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import io.trino.sql.tree.Extract;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractTestExtract
{
    protected QueryAssertions assertions;

    @BeforeClass
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testTestCompleteness()
            throws NoSuchMethodException
    {
        for (Extract.Field value : Extract.Field.values()) {
            String testMethodName = "test" + CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, value.name());
            this.getClass().getMethod(testMethodName);
        }
    }

    @Test
    public void testYear()
    {
        testUnsupportedExtract("YEAR");
    }

    @Test
    public void testQuarter()
    {
        testUnsupportedExtract("QUARTER");
    }

    @Test
    public void testMonth()
    {
        testUnsupportedExtract("MONTH");
    }

    @Test
    public void testWeek()
    {
        testUnsupportedExtract("WEEK");
    }

    @Test
    public void testDay()
    {
        testUnsupportedExtract("DAY");
    }

    @Test
    public void testDayOfMonth()
    {
        testUnsupportedExtract("DAY_OF_MONTH");
    }

    @Test
    public void testDayOfWeek()
    {
        testUnsupportedExtract("DAY_OF_WEEK");
    }

    @Test
    public void testDow()
    {
        testUnsupportedExtract("DOW");
    }

    @Test
    public void testDayOfYear()
    {
        testUnsupportedExtract("DAY_OF_YEAR");
    }

    @Test
    public void testDoy()
    {
        testUnsupportedExtract("DOY");
    }

    @Test
    public void testYearOfWeek()
    {
        testUnsupportedExtract("YEAR_OF_WEEK");
    }

    @Test
    public void testYow()
    {
        testUnsupportedExtract("YOW");
    }

    @Test
    public void testHour()
    {
        testUnsupportedExtract("HOUR");
    }

    @Test
    public void testMinute()
    {
        testUnsupportedExtract("MINUTE");
    }

    @Test
    public void testSecond()
    {
        testUnsupportedExtract("SECOND");
    }

    @Test
    public void testTimezoneMinute()
    {
        testUnsupportedExtract("TIMEZONE_MINUTE");
    }

    @Test
    public void testTimezoneHour()
    {
        testUnsupportedExtract("TIMEZONE_HOUR");
    }

    protected void testUnsupportedExtract(String extractField)
    {
        types().forEach(type -> {
            String expression = format("EXTRACT(%s FROM CAST(NULL AS %s))", extractField, type);
            assertThatThrownBy(assertions.expression(expression)::evaluate, expression)
                    .as(expression)
                    .isInstanceOf(TrinoException.class)
                    .hasMessageMatching(format("line 1:\\d+:\\Q Cannot extract %s from %s", extractField, type));
        });
    }

    protected abstract List<String> types();
}
