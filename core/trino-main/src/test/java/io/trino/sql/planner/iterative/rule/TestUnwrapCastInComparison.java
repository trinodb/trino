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
package io.trino.sql.planner.iterative.rule;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;

import static io.trino.operator.scalar.preimage.TimestampWithTimeZoneCastPreimage.isTimestampToTimestampWithTimeZoneInjectiveAt;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;

public class TestUnwrapCastInComparison
{
    @Test
    public void testIsTimestampToTimestampWithTimeZoneInjectiveAt()
    {
        // UTC, no transitions
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("UTC"), Instant.parse("2020-03-29T00:31:18Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneOffset.UTC, Instant.parse("2020-03-29T00:31:18Z"), true);

        // DST change forward, 2020-03-29 02:00 and 2020-03-29 03:00 local time in Europe/Warsaw both are mapped to 2020-03-29T01:00:00Z
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T00:00:00.999999998Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T00:00:00.999999999Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T01:00:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T01:00:00.000000001Z"), false);

        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T01:00:00.999999998Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T01:00:00.999999999Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T02:00:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T02:00:00.000000001Z"), true);

        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T02:00:00.999999998Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T02:00:00.999999999Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T03:00:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-03-29T03:00:00.000000001Z"), true);

        // Regional rules before 1970 are not used to infer exact cast preimages.
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Africa/Bamako"), Instant.parse("1912-01-01T00:47:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.EPOCH.minusNanos(1), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.EPOCH, true);

        // Fixed offsets have no transitions and support historical values.
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("UTC"), Instant.parse("1912-01-01T00:47:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneOffset.ofHoursMinutes(5, 30), Instant.parse("1912-01-01T00:47:00Z"), true);

        // Decline both occurrences of the repeated local hour, including the transition.
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-24T23:59:59.999999999Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T00:00:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T00:30:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T00:59:59.999999999Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:00:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:30:00Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:59:59.999999999Z"), false);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T02:00:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T02:00:00.000000001Z"), true);

        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T02:00:00.999999998Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T02:00:00.999999999Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T03:00:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T03:00:00.000000001Z"), true);
    }

    private void testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId sessionZone, Instant instant, boolean expected)
    {
        boolean actual = isTimestampToTimestampWithTimeZoneInjectiveAt(sessionZone, instant);
        assertThat(actual)
                .as(format("isTimestampToTimestampWithTimeZoneInjectiveAt(%s, %s)", sessionZone, instant))
                .isEqualTo(expected);
    }
}
