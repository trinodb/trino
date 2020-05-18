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

package io.prestosql.operator.scalar;

import io.prestosql.Session;
import io.prestosql.spi.type.TimestampType;
import org.joda.time.DateTime;
import org.testng.annotations.Test;

import java.time.Instant;

import static io.prestosql.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public class TestDateTimeFunctionsLegacy
        extends TestDateTimeFunctionsBase
{
    public TestDateTimeFunctionsLegacy()
    {
        super(true);
    }

    @Test
    public void testFormatDateCanImplicitlyAddTimeZoneToTimestampLiteral()
    {
        assertFunction("format_datetime(" + TIMESTAMP_LITERAL + ", 'YYYY/MM/dd HH:mm ZZZZ')", VARCHAR, "2001/08/22 03:04 " + DATE_TIME_ZONE.getID());
    }

    @Test
    public void testLocalTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStart(Instant.ofEpochMilli(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis()))
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("LOCALTIMESTAMP", TimestampType.TIMESTAMP, "2017-03-01 14:30:00.000");
        }
    }

    @Test
    public void testCurrentTimestamp()
    {
        Session localSession = Session.builder(session)
                .setStart(Instant.ofEpochMilli(new DateTime(2017, 3, 1, 14, 30, 0, 0, DATE_TIME_ZONE).getMillis()))
                .build();
        try (FunctionAssertions localAssertion = new FunctionAssertions(localSession)) {
            localAssertion.assertFunctionString("CURRENT_TIMESTAMP", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 " + DATE_TIME_ZONE.getID());
            localAssertion.assertFunctionString("NOW()", TIMESTAMP_WITH_TIME_ZONE, "2017-03-01 14:30:00.000 " + DATE_TIME_ZONE.getID());
        }
    }
}
