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
package io.trino.type;

import io.trino.Session;
import io.trino.spi.type.SqlDate;
import io.trino.sql.query.QueryAssertions;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.concurrent.TimeUnit;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.StandardErrorCode.INVALID_LITERAL;
import static io.trino.spi.function.OperatorType.ADD;
import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.joda.time.DateTimeZone.UTC;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestDate
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testLiteral()
    {
        assertThat(assertions.expression("DATE '2001-1-22'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2001, 1, 22, 0, 0, UTC)));

        assertThat(assertions.expression("DATE '2013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        // one digit for month or day
        assertThat(assertions.expression("DATE '2013-2-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertThat(assertions.expression("DATE '2013-02-2'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        // three digit for month or day
        assertTrinoExceptionThrownBy(assertions.expression("DATE '2013-02-002'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2013-02-002' is not a valid DATE literal");

        assertTrinoExceptionThrownBy(assertions.expression("DATE '2013-002-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2013-002-02' is not a valid DATE literal");

        // zero-padded year
        assertThat(assertions.expression("DATE '02013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertThat(assertions.expression("DATE '0013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(13, 2, 2, 0, 0, 0, 0, UTC)));

        // invalid date
        assertTrinoExceptionThrownBy(assertions.expression("DATE '2013-02-29'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2013-02-29' is not a valid DATE literal");

        // surrounding whitespace
        assertThat(assertions.expression("DATE '  2013-02-02  '"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertThat(assertions.expression("DATE ' \t\n\u000B\f\r\u001C\u001D\u001E\u001F 2013-02-02 \t\n\u000B\f\n\u001C\u001D\u001E\u001F '"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        // intra whitespace
        assertTrinoExceptionThrownBy(assertions.expression("DATE '2013 -02-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2013 -02-02' is not a valid DATE literal");

        assertTrinoExceptionThrownBy(assertions.expression("DATE '2013- 2-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '2013- 2-02' is not a valid DATE literal");

        // large year
        assertTrinoExceptionThrownBy(assertions.expression("DATE '5881580-07-12'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '5881580-07-12' is not a valid DATE literal");

        assertTrinoExceptionThrownBy(assertions.expression("DATE '392251590-07-12'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '392251590-07-12' is not a valid DATE literal");

        // signed
        assertThat(assertions.expression("DATE '+2013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertThat(assertions.expression("DATE '-2013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(-2013, 2, 2, 0, 0, 0, 0, UTC)));

        // signed with whitespace
        assertThat(assertions.expression("DATE ' +2013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertTrinoExceptionThrownBy(assertions.expression("DATE '+ 2013-02-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '+ 2013-02-02' is not a valid DATE literal");

        assertTrinoExceptionThrownBy(assertions.expression("DATE ' + 2013-02-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: ' + 2013-02-02' is not a valid DATE literal");

        assertThat(assertions.expression("DATE ' -2013-02-02'"))
                .hasType(DATE)
                .isEqualTo(toDate(new DateTime(-2013, 2, 2, 0, 0, 0, 0, UTC)));

        assertTrinoExceptionThrownBy(assertions.expression("DATE '- 2013-02-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: '- 2013-02-02' is not a valid DATE literal");

        assertTrinoExceptionThrownBy(assertions.expression("DATE ' - 2013-02-02'")::evaluate)
                .hasErrorCode(INVALID_LITERAL)
                .hasMessage("line 1:12: ' - 2013-02-02' is not a valid DATE literal");
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(false);

        assertThat(assertions.operator(EQUAL, "DATE '2001-1-22'", "DATE '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a <> b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.operator(LESS_THAN, "DATE '2001-1-22'", "DATE '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "DATE '2001-1-22'", "DATE '2001-1-20'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(false);

        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-11'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a >= b")
                .binding("a", "DATE '2001-1-22'")
                .binding("b", "DATE '2001-1-23'"))
                .isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-22'")
                .binding("high", "DATE '2001-1-23'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-22'")
                .binding("high", "DATE '2001-1-22'"))
                .isEqualTo(true);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-11'")
                .binding("high", "DATE '2001-1-12'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-23'")
                .binding("high", "DATE '2001-1-24'"))
                .isEqualTo(false);

        assertThat(assertions.expression("value BETWEEN low AND high")
                .binding("value", "DATE '2001-1-22'")
                .binding("low", "DATE '2001-1-23'")
                .binding("high", "DATE '2001-1-11'"))
                .isEqualTo(false);
    }

    @Test
    public void testCastToTimestamp()
    {
        assertThat(assertions.expression("cast(a as timestamp)")
                .binding("a", "DATE '2001-1-22'"))
                .matches("TIMESTAMP '2001-01-22 00:00:00.000'");
    }

    @Test
    public void testCastToTimestampWithTimeZone()
    {
        Session session = testSessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Europe/Berlin"))
                .build();

        assertThat(assertions.expression("CAST(DATE '2001-1-22' AS timestamp with time zone)", session))
                .matches("TIMESTAMP '2001-01-22 00:00:00.000 Europe/Berlin'");
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "DATE '2001-1-22'"))
                .hasType(VARCHAR)
                .isEqualTo("2001-01-22");

        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "DATE '2013-02-02'"))
                .hasType(VARCHAR)
                .isEqualTo("2013-02-02");

        // according to the SQL standard, this literal is incorrect. The required format is 'YYYY-MM-DD'. https://github.com/trinodb/trino/issues/10677
        assertThat(assertions.expression("cast(a as varchar)")
                .binding("a", "DATE '13-2-2'"))
                .hasType(VARCHAR)
                .isEqualTo("0013-02-02");

        assertThat(assertions.expression("cast(a as varchar(50))")
                .binding("a", "DATE '2013-02-02'"))
                .hasType(createVarcharType(50))
                .isEqualTo("2013-02-02");

        assertThat(assertions.expression("cast(a as varchar(10))")
                .binding("a", "DATE '2013-02-02'"))
                .hasType(createVarcharType(10))
                .isEqualTo("2013-02-02");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as varchar(9))")
                .binding("a", "DATE '2013-02-02'").evaluate())
                .hasMessage("Value 2013-02-02 cannot be represented as varchar(9)")
                .hasErrorCode(INVALID_CAST_ARGUMENT);
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'2001-1-22'"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'\n\t 2001-1-22'"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'2001-1-22 \t\n'"))
                .matches("DATE '2001-01-22'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'\n\t 2001-1-22 \t\n'"))
                .matches("DATE '2001-01-22'");

        // Note: update DomainTranslator.Visitor.createVarcharCastToDateComparisonExtractionResult whenever CAST behavior changes.
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'2013-02-02'"))
                .matches("DATE '2013-02-02'");

        // one digit for month or day
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'2013-2-02'"))
                .matches("DATE '2013-02-02'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'2013-02-2'"))
                .matches("DATE '2013-02-02'");

        // three digit for month or day
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'2013-02-002'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 2013-02-002");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'2013-002-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 2013-002-02");

        // zero-padded year
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'02013-02-02'"))
                .matches("DATE '2013-02-02'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'0013-02-02'"))
                .matches("DATE '13-02-02'");

        // invalid date
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'2013-02-29'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 2013-02-29");

        // surrounding whitespace
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'  2013-02-02  '"))
                .matches("DATE '2013-02-02'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "' \t\n\u000B\f\r\u001C\u001D\u001E\u001F 2013-02-02 \t\n\u000B\f\n\u001C\u001D\u001E\u001F '"))
                .matches("DATE '2013-02-02'");

        // intra whitespace
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'2013 -02-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 2013 -02-02");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'2013- 2-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 2013- 2-02");

        // large year
        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'5881580-07-12'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 5881580-07-12");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'392251590-07-12'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: 392251590-07-12");

        // signed
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'+2013-02-02'"))
                .matches("DATE '2013-02-02'");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "'-2013-02-02'"))
                .matches("DATE '-2013-02-02'");

        // signed with whitespace
        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "' +2013-02-02'"))
                .matches("DATE '2013-02-02'");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'+ 2013-02-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: + 2013-02-02");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "' + 2013-02-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date:  + 2013-02-02");

        assertThat(assertions.expression("cast(a as date)")
                .binding("a", "' -2013-02-02'"))
                .matches("DATE '-2013-02-02'");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "'- 2013-02-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date: - 2013-02-02");

        assertTrinoExceptionThrownBy(() -> assertions.expression("cast(a as date)")
                .binding("a", "' - 2013-02-02'").evaluate())
                .hasErrorCode(INVALID_CAST_ARGUMENT)
                .hasMessage("Value cannot be cast to date:  - 2013-02-02");
    }

    @Test
    public void testGreatest()
    {
        assertThat(assertions.function("greatest", "DATE '2013-03-30'", "DATE '2012-05-23'"))
                .matches("DATE '2013-03-30'");

        assertThat(assertions.function("greatest", "DATE '2013-03-30'", "DATE '2012-05-23'", "DATE '2012-06-01'"))
                .matches("DATE '2013-03-30'");
    }

    @Test
    public void testLeast()
    {
        assertThat(assertions.function("least", "DATE '2013-03-30'", "DATE '2012-05-23'"))
                .matches("DATE '2012-05-23'");

        assertThat(assertions.function("least", "DATE '2013-03-30'", "DATE '2012-05-23'", "DATE '2012-06-01'"))
                .matches("DATE '2012-05-23'");
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "CAST(NULL AS DATE)"))
                .isEqualTo(true);

        assertThat(assertions.operator(INDETERMINATE, "DATE '2013-10-27'"))
                .isEqualTo(false);
    }

    @Test
    public void testIsDistinctFrom()
    {
        assertThat(assertions.operator(IS_DISTINCT_FROM, "CAST(NULL AS DATE)", "CAST(NULL AS DATE)"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DATE '2013-10-27'", "TIMESTAMP '2013-10-27 00:00:00'"))
                .isEqualTo(false);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DATE '2013-10-27'", "TIMESTAMP '2013-10-28 00:00:00'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "NULL", "DATE '2013-10-27'"))
                .isEqualTo(true);

        assertThat(assertions.operator(IS_DISTINCT_FROM, "DATE '2013-10-27'", "NULL"))
                .isEqualTo(true);
    }

    @Test
    public void testDateToTimestampCoercing()
    {
        assertThat(assertions.operator(EQUAL, "DATE '2013-10-27'", "TIMESTAMP '2013-10-27 00:00:00'"))
                .isEqualTo(true);

        assertThat(assertions.operator(LESS_THAN, "DATE '2013-10-27'", "TIMESTAMP '2013-10-27 00:00:01'"))
                .isEqualTo(true);

        assertThat(assertions.expression("a > b")
                .binding("a", "DATE '2013-10-27'")
                .binding("b", "TIMESTAMP '2013-10-26 23:59:59'"))
                .isEqualTo(true);
    }

    @Test
    public void testDateToTimestampWithZoneCoercing()
    {
        Session session = assertions.sessionBuilder()
                .setTimeZoneKey(getTimeZoneKey("Europe/Berlin"))
                .build();

        assertThat(assertions.expression("DATE '2013-10-27' = TIMESTAMP '2013-10-27 00:00:00 Europe/Berlin'", session))
                .isEqualTo(true);

        assertThat(assertions.expression("DATE '2013-10-27' < TIMESTAMP '2013-10-27 00:00:01 Europe/Berlin'", session))
                .isEqualTo(true);

        assertThat(assertions.expression("DATE '2013-10-27' > TIMESTAMP '2013-10-26 23:59:59 Europe/Berlin'", session))
                .isEqualTo(true);
    }

    @Test
    public void testMinusInterval()
    {
        assertThat(assertions.operator(SUBTRACT, "DATE '2001-1-22'", "INTERVAL '3' day"))
                .matches("DATE '2001-01-19'");

        assertTrinoExceptionThrownBy(assertions.operator(SUBTRACT, "DATE '2001-1-22'", "INTERVAL '3' hour")::evaluate)
                .hasMessage("Cannot subtract hour, minutes or seconds from a date");
    }

    @Test
    public void testPlusInterval()
    {
        assertThat(assertions.operator(ADD, "DATE '2001-1-22'", "INTERVAL '3' day"))
                .matches("DATE '2001-01-25'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' day", "DATE '2001-1-22'"))
                .matches("DATE '2001-01-25'");

        assertThat(assertions.operator(ADD, "DATE '2001-1-22'", "INTERVAL '3' month"))
                .matches("DATE '2001-04-22'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' month", "DATE '2001-1-22'"))
                .matches("DATE '2001-04-22'");

        assertThat(assertions.operator(ADD, "DATE '2001-1-22'", "INTERVAL '3' year"))
                .matches("DATE '2004-01-22'");

        assertThat(assertions.operator(ADD, "INTERVAL '3' year", "DATE '2001-1-22'"))
                .matches("DATE '2004-01-22'");

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "DATE '2001-1-22'", "INTERVAL '3' hour")::evaluate)
                .hasMessage("Cannot add hour, minutes or seconds to a date");

        assertTrinoExceptionThrownBy(assertions.operator(ADD, "INTERVAL '3' hour", "DATE '2001-1-22'")::evaluate)
                .hasMessage("Cannot add hour, minutes or seconds to a date");
    }

    private static SqlDate toDate(DateTime dateTime)
    {
        return new SqlDate((int) TimeUnit.MILLISECONDS.toDays(dateTime.getMillis()));
    }
}
