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
package io.trino.plugin.deltalake;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;
import io.trino.Session;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.chrono.IsoChronology;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.format.SignStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.DELTA_CATALOG;
import static io.trino.plugin.deltalake.DeltaLakeQueryRunner.createDeltaLakeQueryRunner;
import static io.trino.spi.type.TimeZoneKey.UTC_KEY;
import static io.trino.spi.type.TimeZoneKey.getTimeZoneKey;
import static java.lang.String.format;
import static java.lang.String.join;
import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;
import static java.util.stream.Collectors.joining;

public class TestDeltaLakeReadTimestamps
        extends AbstractTestQueryFramework
{
    private static final Logger log = Logger.get(TestDeltaLakeReadTimestamps.class);

    private static final ZoneId UTC = ZoneId.of("UTC");
    private static final ZoneId TEST_TIME_ZONE = ZoneId.of("America/Bahia_Banderas");
    private static final DateTimeFormatter EXPECTED_VALUES_FORMATTER = new DateTimeFormatterBuilder()
            // This is equivalent to appendInstant(3) (yyyy-MM-ddTHH:mm:ss.SSS with 3-9 fractional digits),
            // but without a plus sign for years greater than 9999.
            .appendLiteral('\'')
            .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
            .appendLiteral('-')
            .appendValue(MONTH_OF_YEAR, 2)
            .appendLiteral('-')
            .appendValue(DAY_OF_MONTH, 2)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2).appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2).appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .appendFraction(ChronoField.NANO_OF_SECOND, 3, 9, true)
            .appendLiteral('Z')
            .appendLiteral('\'')
            .toFormatter(Locale.ENGLISH)
            .withChronology(IsoChronology.INSTANCE)
            .withResolverStyle(ResolverStyle.STRICT);
    private static final DateTimeFormatter WHERE_VALUE_FORMATTER = DateTimeFormatter.ofPattern("'TIMESTAMP '''yyyy-MM-dd HH:mm:ss.SSS VV''").withZone(UTC);

    /**
     * Return the JVM time zone if it is the test time zone, otherwise throw an
     * {@code IllegalStateException}.
     */
    public static ZoneId getJvmTestTimeZone()
    {
        ZoneId zone = ZoneId.systemDefault();
        checkState(TEST_TIME_ZONE.equals(zone), "Assumed JVM time zone was " +
                TEST_TIME_ZONE.getId() + ", but found " + zone.getId());
        return zone;
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return createDeltaLakeQueryRunner(DELTA_CATALOG, ImmutableMap.of(), ImmutableMap.of("delta.register-table-procedure.enabled", "true"));
    }

    @BeforeClass
    public void registerTables()
    {
        String dataPath = getClass().getClassLoader().getResource("databricks/read_timestamps").toExternalForm();
        getQueryRunner().execute(format("CALL system.register_table('%s', 'read_timestamps', '%s')", getSession().getSchema().orElseThrow(), dataPath));
    }

    @Test
    public void timestampReadMapping()
    {
        ZoneId jvmZone = getJvmTestTimeZone();
        verify(jvmZone.getRules().getValidOffsets(LocalDateTime.parse("1970-01-01T00:05:00.123")).isEmpty());
        verify(jvmZone.getRules().getValidOffsets(LocalDateTime.parse("1996-10-27T01:05:00.987")).size() == 2);

        ZoneId vilniusZone = ZoneId.of("Europe/Vilnius");
        verify(vilniusZone.getRules().getValidOffsets(LocalDateTime.parse("1983-04-01T00:05:00.345")).isEmpty());
        verify(vilniusZone.getRules().getValidOffsets(LocalDateTime.parse("1983-09-30T23:59:00.654")).size() == 2);

        ImmutableList.Builder<Instant> testCases = ImmutableList.builder();
        testCases.add(Instant.parse("1900-01-01T00:00:00.000Z"));
        testCases.add(Instant.parse("1952-04-03T01:02:03.456Z"));
        testCases.add(Instant.parse("1970-01-01T00:00:00.000Z"));
        testCases.add(Instant.parse("1970-02-03T04:05:06.789Z"));
        testCases.add(Instant.parse("2017-07-01T00:00:00.000Z"));
        testCases.add(Instant.parse("1970-01-01T00:05:00.123Z")); // local time (in UTC) of this instant is a gap in test JVM zone
        testCases.add(Instant.parse("1969-12-31T23:05:00.123Z"));
        testCases.add(Instant.parse("1970-01-01T01:05:00.123Z"));
        testCases.add(Instant.parse("1996-10-27T01:05:00.987Z")); // local time (in UTC) of this instant is has 2 valid offsets in JVM zone
        testCases.add(Instant.parse("1996-10-27T00:05:00.987Z"));
        testCases.add(Instant.parse("1996-10-27T02:05:00.987Z"));
        testCases.add(Instant.parse("1983-04-01T00:05:00.345Z")); // local time (in UTC) of this instant is a gap in Europe/Vilnius
        testCases.add(Instant.parse("1983-03-31T23:05:00.345Z"));
        testCases.add(Instant.parse("1983-04-01T01:05:00.345Z"));
        testCases.add(Instant.parse("1983-09-30T23:59:00.654Z")); // local time (in UTC) of this instant is has 2 valid offsets in Europe/Vilnius
        testCases.add(Instant.parse("1983-09-30T22:59:00.654Z"));
        testCases.add(Instant.parse("1983-10-01T00:59:00.654Z"));
        testCases.add(Instant.parse("9999-12-31T23:59:59.999Z"));

        for (String zone : ImmutableList.of(UTC_KEY.getId(), jvmZone.getId(), vilniusZone.getId())) {
            runTestInTimeZone(zone, testCases.build());
        }
    }

    private void runTestInTimeZone(String zone, List<Instant> testCases)
    {
        log.info("Starting test in time zone %s", zone);
        Session.SessionBuilder sessionBuilder = Session.builder(getQueryRunner().getDefaultSession());
        if (zone != null) {
            sessionBuilder.setTimeZoneKey(getTimeZoneKey(zone));
        }
        String projections = IntStream.range(1, 37)
                .mapToObj(columnIndex -> format("to_iso8601(col_%s)", columnIndex))
                .collect(joining(", "));
        String expectedPartitionedTimestamps = testCases.stream()
                .map(x -> EXPECTED_VALUES_FORMATTER.format(x.atZone(ZoneId.of(zone)).withZoneSameLocal(UTC)))
                .collect(joining(", "));
        String expectedValues = testCases.stream()
                .map(x -> EXPECTED_VALUES_FORMATTER.format(x.atZone(UTC)))
                .collect(joining(", "));
        String expected = "VALUES ('" + zone + "', " + expectedPartitionedTimestamps + ", " + expectedValues + ")";
        String actual = "SELECT col_0, " + projections + " FROM read_timestamps WHERE col_0 = '" + zone + "'";
        Session session = sessionBuilder.build();
        assertQuery(session, actual, expected);
        actual = "SELECT col_0, " + projections + " FROM read_timestamps WHERE " + buildTrinoWhereClauses(zone, testCases);
        assertQuery(session, actual, expected);
    }

    private String buildTrinoWhereClauses(String zone, List<Instant> testCases)
    {
        List<String> predicates = new ArrayList<>();
        // partition values
        for (int i = 0; i < testCases.size(); i++) {
            Instant stored = testCases.get(i).atZone(ZoneId.of(zone)).withZoneSameLocal(UTC).toInstant();
            predicates.add(format("col_%d = %s", i + 1, WHERE_VALUE_FORMATTER.format(stored)));
        }
        // Parquet values
        for (int i = 0; i < testCases.size(); i++) {
            Instant stored = testCases.get(i);
            predicates.add(format("col_%d = %s", testCases.size() + i + 1, WHERE_VALUE_FORMATTER.format(stored)));
        }
        return join(" AND ", predicates);
    }
}
