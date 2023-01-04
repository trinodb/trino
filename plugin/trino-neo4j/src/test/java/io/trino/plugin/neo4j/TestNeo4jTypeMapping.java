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
package io.trino.plugin.neo4j;

import com.google.common.collect.ImmutableList;
import io.trino.Session;
import io.trino.plugin.base.util.JsonUtils;
import io.trino.spi.type.TimeZoneKey;
import io.trino.spi.type.Type;
import io.trino.sql.query.QueryAssertions;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.MaterializedRow;
import io.trino.testing.QueryRunner;
import io.trino.testing.datatype.SqlDataTypeTest;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimeWithTimeZoneType.createTimeWithTimeZoneType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createUnboundedVarcharType;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;
import static java.time.temporal.ChronoField.HOUR_OF_DAY;
import static java.time.temporal.ChronoField.MINUTE_OF_HOUR;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.time.temporal.ChronoField.SECOND_OF_MINUTE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNeo4jTypeMapping
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        TestingNeo4jServer neo4jServer = closeAfterClass(new TestingNeo4jServer());
        return Neo4jQueryRunner.createDefaultQueryRunner(neo4jServer);
    }

    @Test
    public void testBoolean()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("boolean", "true", BOOLEAN, "true")
                .addRoundTrip("boolean", "True", BOOLEAN, "true")
                .addRoundTrip("boolean", "false", BOOLEAN, "false")
                .addRoundTrip("boolean", "False", BOOLEAN, "false")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testBigint()
    {
        // neo4j supports Long values using 'integer' data type
        SqlDataTypeTest.create()
                .addRoundTrip("integer", "-9223372036854775808", BIGINT, "-9223372036854775808") // min value in MySQL and Trino
                .addRoundTrip("integer", "123456789012", BIGINT, "123456789012")
                .addRoundTrip("integer", "9223372036854775807", BIGINT, "9223372036854775807") // max value in MySQL and Trino
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testFloat()
    {
        // neo4j maps JDBC float to Java Double
        SqlDataTypeTest.create()
                .addRoundTrip("float", "3.14", DOUBLE, "CAST (3.14 as double)")
                .addRoundTrip("float", "10.3e0", DOUBLE, "10.3e0")
                .addRoundTrip("float", "1.0E100", DOUBLE, "1.0E100")
                .addRoundTrip("float", "1.23456E12", DOUBLE, "1.23456E12")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testNull()
    {
        // neo4j maps JDBC float to Java Double
        SqlDataTypeTest.create()
                .addRoundTrip("null", "NULL", VARCHAR, "CAST (null as varchar)")
                .addRoundTrip("null", "Null", VARCHAR, "CAST (null as varchar)")
                .addRoundTrip("null", "null", VARCHAR, "CAST (null as varchar)")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testString()
    {
        // neo4j doesn't support bounded varchars
        SqlDataTypeTest.create()
                .addRoundTrip("string", "''text_a''", createUnboundedVarcharType(), "CAST('text_a' AS varchar)")
                .addRoundTrip("string", "''攻殻機動隊''", createUnboundedVarcharType(), "CAST('攻殻機動隊' AS varchar)")
                .addRoundTrip("string", "''😂''", createUnboundedVarcharType(), "CAST('😂' AS varchar)")
                .addRoundTrip("string", "''Ну, погоди!''", createUnboundedVarcharType(), "CAST('Ну, погоди!' AS varchar)")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testDate()
    {
        SqlDataTypeTest.create()
                .addRoundTrip("date", "date(''0001-01-01'')", DATE, "DATE '0001-01-01'")
                .addRoundTrip("date", "date(''1582-10-04'')", DATE, "DATE '1582-10-04'") // before julian->gregorian switch
                .addRoundTrip("date", "date(''1582-10-05'')", DATE, "DATE '1582-10-05'") // begin julian->gregorian switch
                .addRoundTrip("date", "date(''1582-10-14'')", DATE, "DATE '1582-10-14'") // end julian->gregorian switch
                .addRoundTrip("date", "date(''1952-04-03'')", DATE, "DATE '1952-04-03'") // before epoch
                .addRoundTrip("date", "date(''1970-01-01'')", DATE, "DATE '1970-01-01'")
                .addRoundTrip("date", "date(''1970-02-03'')", DATE, "DATE '1970-02-03'")
                .addRoundTrip("date", "date(''2017-07-01'')", DATE, "DATE '2017-07-01'") // summer on northern hemisphere (possible DST)
                .addRoundTrip("date", "date(''2017-01-01'')", DATE, "DATE '2017-01-01'") // winter on northern hemisphere (possible DST on southern hemisphere)
                .addRoundTrip("date", "date(''1983-04-01'')", DATE, "DATE '1983-04-01'")
                .addRoundTrip("date", "date(''1983-10-01'')", DATE, "DATE '1983-10-01'")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testLocalTime()
    {
        // neo4j doesn't set any precision and can have upto max precision of 9
        int precision = 9;
        SqlDataTypeTest.create()
                .addRoundTrip("localtime", "localtime(''00:00:00'')", createTimeType(precision), "TIME '00:00:00.000000000'")
                .addRoundTrip("localtime", "localtime(''00:00:00.000000'')", createTimeType(precision), "TIME '00:00:00.000000000'")
                .addRoundTrip("localtime", "localtime(''00:00:00.123456'')", createTimeType(precision), "TIME '00:00:00.123456000'")
                .addRoundTrip("localtime", "localtime(''12:34:56'')", createTimeType(precision), "TIME '12:34:56.000000000'")
                .addRoundTrip("localtime", "localtime(''12:34:56.123456'')", createTimeType(precision), "TIME '12:34:56.123456000'")

                // testing different precision values
                .addRoundTrip("localtime", "localtime(''23:59:59'')", createTimeType(precision), "TIME '23:59:59.000000000'")
                .addRoundTrip("localtime", "localtime(''23:59:59.999'')", createTimeType(precision), "TIME '23:59:59.999000000'")
                .addRoundTrip("localtime", "localtime(''23:59:59.999999'')", createTimeType(precision), "TIME '23:59:59.999999000'")
                .addRoundTrip("localtime", "localtime(''23:59:59.999999999'')", createTimeType(precision), "TIME '23:59:59.999999999'")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testTimeWithOffset()
    {
        Session session = Session.builder(this.getQueryRunner().getDefaultSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(ZoneOffset.UTC.getId()))
                .build();
        // neo4j doesn't set any precision and can have upto max precision of 9
        int precision = 9;
        SqlDataTypeTest.create()
                .addRoundTrip("time", "time(''00:00:00'')", createTimeWithTimeZoneType(precision), "TIME '00:00:00.000000000+00:00'")
                .addRoundTrip("time", "time(''00:00:00+02:00'')", createTimeWithTimeZoneType(precision), "TIME '00:00:00.000000000+02:00'")
                .addRoundTrip("time", "time(''00:00:00-02:00'')", createTimeWithTimeZoneType(precision), "TIME '00:00:00.000000000-02:00'")
                .addRoundTrip("time", "time(''00:00:00.123456-02:00'')", createTimeWithTimeZoneType(precision), "TIME '00:00:00.123456000-02:00'")
                .addRoundTrip("time", "time(''00:00:01.123456-14:00'')", createTimeWithTimeZoneType(precision), "TIME '00:00:01.123456000-14:00'")

                .addRoundTrip("time", "time(''23:59:59+14:00'')", createTimeWithTimeZoneType(precision), "TIME '23:59:59.000000000+14:00'")
                .addRoundTrip("time", "time(''23:59:59.999999999+14:00'')", createTimeWithTimeZoneType(precision), "TIME '23:59:59.999999999+14:00'")
                .addRoundTrip("time", "time(''23:59:59-14:00'')", createTimeWithTimeZoneType(precision), "TIME '23:59:59.000000000-14:00'")
                .addRoundTrip("time", "time(''23:59:59.999999999-14:00'')", createTimeWithTimeZoneType(precision), "TIME '23:59:59.999999999-14:00'")

                .addRoundTrip("time", "time(''12:34:56+14:00'')", createTimeWithTimeZoneType(precision), "TIME '12:34:56.000000000+14:00'")
                .addRoundTrip("time", "time(''12:34:56.999999999-14:00'')", createTimeWithTimeZoneType(precision), "TIME '12:34:56.999999999-14:00'")
                .execute(this.getQueryRunner(), session, new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testLocalDateTime()
    {
        int precision = 9;
        SqlDataTypeTest.create()
                .addRoundTrip("localdatetime", "localdatetime(''1958-01-01T13:18:03.123'')", createTimestampType(9), "TIMESTAMP '1958-01-01 13:18:03.123000000'")
                // after epoch
                .addRoundTrip("localdatetime", "localdatetime(''2019-03-18T10:01:17.987'')", createTimestampType(9), "TIMESTAMP '2019-03-18 10:01:17.987000000'")
                // time doubled in JVM zone
                .addRoundTrip("localdatetime", "localdatetime(''2018-10-28T01:33:17.456'')", createTimestampType(9), "TIMESTAMP '2018-10-28 01:33:17.456000000'")
                // time double in Vilnius
                .addRoundTrip("localdatetime", "localdatetime(''2018-10-28T03:33:33.333'')", createTimestampType(9), "TIMESTAMP '2018-10-28 03:33:33.333000000'")
                // epoch
                .addRoundTrip("localdatetime", "localdatetime(''1970-01-01T00:00:00.000'')", createTimestampType(9), "TIMESTAMP '1970-01-01 00:00:00.000000000'")
                // time gap in JVM zone
                .addRoundTrip("localdatetime", "localdatetime(''1970-01-01T00:13:42.000'')", createTimestampType(9), "TIMESTAMP '1970-01-01 00:13:42.000000000'")
                .addRoundTrip("localdatetime", "localdatetime(''2018-04-01T02:13:55.123'')", createTimestampType(9), "TIMESTAMP '2018-04-01 02:13:55.123000000'")
                // time gap in Vilnius
                .addRoundTrip("localdatetime", "localdatetime(''2018-03-25T03:17:17.000'')", createTimestampType(9), "TIMESTAMP '2018-03-25 03:17:17.000000000'")
                // time gap in Kathmandu
                .addRoundTrip("localdatetime", "localdatetime(''1986-01-01T00:13:07.000'')", createTimestampType(9), "TIMESTAMP '1986-01-01 00:13:07.000000000'")
                .execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testDateTimeWithTimeZone()
    {
        Type trinoType = createTimestampWithTimeZoneType(9);
        DateTimeFormatter trinoFormatter = new DateTimeFormatterBuilder()
                .append(ISO_LOCAL_DATE)
                .appendLiteral(" ")
                .appendValue(HOUR_OF_DAY, 2)
                .appendLiteral(':')
                .appendValue(MINUTE_OF_HOUR, 2)
                .appendLiteral(':')
                .appendValue(SECOND_OF_MINUTE, 2)
                .appendFraction(NANO_OF_SECOND, 9, 9, true)
                .appendLiteral(" ")
                .appendZoneRegionId().toFormatter();
        // different time zones to test
        List<String> timeZoneIds = List.of(
                "UTC",
                "Europe/Vilnius",
                "Asia/Kathmandu");
        List<LocalDateTime> dateTimes = List.of(
                // before epoch
                LocalDateTime.of(1958, 1, 1, 13, 18, 3, 123_000_000),
                // epoch
                LocalDateTime.of(1970, 1, 1, 0, 0, 0),
                // after epoch
                LocalDateTime.of(2019, 3, 18, 10, 1, 17, 987_000_000),
                // timeGapInJvmZone1
                LocalDateTime.of(1970, 1, 1, 0, 13, 42),
                // timeGapInJvmZone2
                LocalDateTime.of(2018, 4, 1, 2, 13, 55, 123_000_000),
                // timeDoubledInJvmZone
                LocalDateTime.of(2018, 10, 28, 1, 33, 17, 456_000_000),
                // no DST in 1970, but has DST in later years (e.g. 2018)
                LocalDateTime.of(2018, 3, 25, 3, 17, 17),
                LocalDateTime.of(2018, 10, 28, 3, 33, 33, 333_000_000),
                // minutes offset change since 1970-01-01, no DST
                LocalDateTime.of(1986, 1, 1, 0, 13, 7));
        SqlDataTypeTest test = SqlDataTypeTest.create();
        for (String timeZoneId : timeZoneIds) {
            for (LocalDateTime dateTime : dateTimes) {
                ZonedDateTime zonedDateTime = dateTime.atZone(ZoneId.of(timeZoneId));
                test
                        // test with only date part
                        .addRoundTrip("datetime", String.format("datetime(''%s'')", dateTime.format(ISO_LOCAL_DATE)), trinoType, String.format("TIMESTAMP '%s'", dateTime.truncatedTo(ChronoUnit.DAYS).atZone(ZoneId.of("UTC")).format(trinoFormatter)))
//                        // test excluding the time zone part
                        .addRoundTrip("datetime", String.format("datetime(''%s'')", dateTime.format(ISO_LOCAL_DATE_TIME)), trinoType, String.format("TIMESTAMP '%s'", dateTime.atZone(ZoneId.of("UTC")).format(trinoFormatter)))
                        // test with time zone included
                        .addRoundTrip("datetime", String.format("datetime(''%s[%s]'')", dateTime.format(ISO_LOCAL_DATE_TIME), timeZoneId), trinoType, String.format("TIMESTAMP '%s'", zonedDateTime.format(trinoFormatter)));
            }
        }
        test.execute(this.getQueryRunner(), new Neo4jTableFunctionDataSetup(Optional.empty()));
    }

    @Test
    public void testNodeType()
    {
        new Neo4jTypeValidator()
                .addTestCase(new Neo4jTypeTestCase("MATCH (m:Movie) where m.title = ''Bourne Identity'' return m", VARCHAR, PropExtractMode.Map, ImmutableList.of("title", "_labels"),
                        ImmutableList.of("Bourne Identity", ImmutableList.of("Movie"))))
                .addTestCase(new Neo4jTypeTestCase("MATCH (m:Movie) where m.title = ''Bourne Identity2'' return m", VARCHAR, PropExtractMode.Map, ImmutableList.of(), ImmutableList.of()))
                .validate();
    }

    @Test
    public void testRelationshipType()
    {
        new Neo4jTypeValidator()
                .addTestCase(new Neo4jTypeTestCase("MATCH (people:Person)-[relation:DIRECTED]-(:Movie {title: \"Mission Impossible\"}) RETURN relation", VARCHAR, PropExtractMode.Map, "_type", "DIRECTED"))
                .validate();
    }

    @Test
    public void testPathType()
    {
        new Neo4jTypeValidator()
                .addTestCase(new Neo4jTypeTestCase("MATCH p = (:Person{name: ''Tom Cruise''})-[:ACTED_IN*2]-() RETURN p", VARCHAR, PropExtractMode.List, ImmutableList.of("0:name", "1:_type", "2:title", "3:_type", "4:name"), ImmutableList.of("Tom Cruise", "ACTED_IN", "Mission Impossible", "ACTED_IN", "Jon Voight")))
                .validate();
    }

    @Test
    public void testMapType()
    {
        new Neo4jTypeValidator()
                .addTestCase(new Neo4jTypeTestCase("RETURN {stringType: ''string_value'', intType: 100, booleanType: True, nullType: NULL}", VARCHAR, PropExtractMode.Map, ImmutableList.of("stringType", "intType", "booleanType", "nullType"), Stream.of("string_value", 100, true, null).collect(Collectors.toList())))
                .validate();
    }

    @Test
    public void testArrayType()
    {
        new Neo4jTypeValidator()
                .addTestCase(new Neo4jTypeTestCase("RETURN [''string_value'', 100, true, NULL]", VARCHAR, PropExtractMode.List, ImmutableList.of("0", "1", "2", "3"), Stream.of("string_value", 100, true, null).collect(Collectors.toList())))
                .validate();
    }

    private String buildTableFunctionQuery(String cypherQuery)
    {
        return String.format("select * from TABLE(neo4j.system.query(query => '%s'))", cypherQuery);
    }

    private enum PropExtractMode
    {
        None, List, Map
    }

    public class Neo4jTypeTestCase
    {
        private String cypherSql;
        private List<Type> expectedTypes;
//        private boolean extractAsList;
        private PropExtractMode propExtractMode = PropExtractMode.None;
        private List<String> extractProps;
        private List<Object> expectedLiterals;

        public Neo4jTypeTestCase(String cypherSql, Type expectedType, Object expectedLiteral)
        {
            this(cypherSql, ImmutableList.of(expectedType), PropExtractMode.None, ImmutableList.of(), ImmutableList.of(expectedLiteral));
        }

        public Neo4jTypeTestCase(String cypherSql, Type expectedType, PropExtractMode propExtractMode, List<String> extractProps, List<Object> expectedLiterals)
        {
            this(cypherSql, ImmutableList.of(expectedType), propExtractMode, extractProps, expectedLiterals);
        }

        public Neo4jTypeTestCase(String cypherSql, Type expectedType, PropExtractMode propExtractMode, String extractProp, Object expectedLiteral)
        {
            this(cypherSql, ImmutableList.of(expectedType), propExtractMode, ImmutableList.of(extractProp), ImmutableList.of(expectedLiteral));
        }

        public Neo4jTypeTestCase(String cypherSql, List<Type> expectedTypes, PropExtractMode propExtractMode, List<String> extractProps, List<Object> expectedLiterals)
        {
            this.cypherSql = requireNonNull(cypherSql, "cypherSql is null");
            this.expectedTypes = requireNonNull(expectedTypes, "expectedTypes are null");
            this.propExtractMode = requireNonNull(propExtractMode, "propExtractMode is null");
            this.extractProps = requireNonNull(extractProps, "extractProps is null");
            this.expectedLiterals = requireNonNull(expectedLiterals, "expectedLiterals is null");
        }
    }

    public class Neo4jTypeValidator
    {
        private List<Neo4jTypeTestCase> testCases;

        public Neo4jTypeValidator addTestCase(Neo4jTypeTestCase testCase)
        {
            if (this.testCases == null) {
                this.testCases = new ArrayList<>();
            }
            this.testCases.add(testCase);
            return this;
        }

        public void validate()
        {
            for (Neo4jTypeTestCase testCase : testCases) {
                this.validateTestCase(testCase);
            }
        }

        private void validateTestCase(Neo4jTypeTestCase testCase)
        {
            String selectQuery = buildTableFunctionQuery(testCase.cypherSql);
            QueryAssertions queryAssertions = new QueryAssertions(getQueryRunner());
            QueryAssertions.QueryAssert assertion = assertThat(queryAssertions.query(selectQuery));
            int index = 0;
            for (Type expectedType : testCase.expectedTypes) {
                assertion.outputHasType(index++, expectedType);
            }
            // validate the literals
            MaterializedResult actualResult = queryAssertions.execute(selectQuery);
            if (testCase.expectedLiterals.size() > 0) {
                MaterializedRow row = actualResult.getMaterializedRows().get(0);
                List<Object> actualLiterals;
                if (testCase.propExtractMode == PropExtractMode.List) {
                    List<Object> actualResultList = JsonUtils.parseJson((String) row.getField(0), List.class);
                    actualLiterals = testCase.extractProps.stream().map(propName -> {
                        Object resultObject = actualResultList.get(Integer.parseInt(propName.split(":")[0]));
                        if (resultObject instanceof Map) {
                            return ((Map) resultObject).get(propName.split(":")[1]);
                        }
                        else {
                            return resultObject;
                        }
                    }).collect(Collectors.toList());
                }
                else if (testCase.propExtractMode == PropExtractMode.Map) {
                    Map<String, Object> actualResultMap = JsonUtils.parseJson((String) row.getField(0), Map.class);
                    actualLiterals = testCase.extractProps.stream().map(propName -> actualResultMap.get(propName)).collect(Collectors.toList());
                }
                else {
                    actualLiterals = ImmutableList.of(row.getField(0));
                }
                assertEquals(actualLiterals, testCase.expectedLiterals);
            }
            else {
                assertTrue(actualResult.getMaterializedRows().size() == 0);
            }
        }
    }
}
