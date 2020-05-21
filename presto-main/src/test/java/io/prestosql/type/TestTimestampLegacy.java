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
package io.prestosql.type;

import io.prestosql.Session;
import io.prestosql.spi.type.TimeZoneKey;
import org.testng.annotations.Test;

import static io.prestosql.spi.StandardErrorCode.INVALID_LITERAL;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.testing.DateTimeTestingUtils.sqlTimestampOf;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.assertions.PrestoExceptionAssert.assertPrestoExceptionThrownBy;

public class TestTimestampLegacy
        extends TestTimestampBase
{
    public TestTimestampLegacy()
    {
        super(true);
    }

    @Override
    public void testCastFromSlice()
    {
        super.testCastFromSlice();
        assertFunction(
                "cast('2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 8, 55, 5, 321, session));
        assertFunction(
                "cast('2001-1-22 03:04:05 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 8, 55, 5, 0, session));
        assertFunction(
                "cast('2001-1-22 03:04 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 8, 55, 0, 0, session));
        assertFunction(
                "cast('2001-1-22 +07:09' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 5, 51, 0, 0, session));

        assertFunction(
                "cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 12, 4, 5, 321, session));
        assertFunction(
                "cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 12, 4, 5, 0, session));
        assertFunction(
                "cast('2001-1-22 03:04 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 12, 4, 0, 0, session));
        assertFunction(
                "cast('2001-1-22 Asia/Oral' as timestamp)",
                TIMESTAMP,
                sqlTimestampOf(2001, 1, 21, 9, 0, 0, 0, session));
    }

    @Test
    public void testInvalidLiteral()
    {
        Session session = testSessionBuilder()
                .setSystemProperty("legacy_timestamp", "true")
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("Europe/Vilnius"))
                .build();

        assertPrestoExceptionThrownBy(() -> functionAssertions.tryEvaluate("TIMESTAMP '2018-03-25 03:17:17.000'", TIMESTAMP, session))
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:1: '2018-03-25 03:17:17.000' is not a valid timestamp literal");
    }
}
