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

import static io.trino.sql.planner.iterative.rule.UnwrapCastInComparison.isTimestampToTimestampWithTimeZoneInjectiveAt;
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

        // DST change backward
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T00:00:00.999999998Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T00:00:00.999999999Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:00:00Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:00:00.000000001Z"), true);

        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:00:00.999999998Z"), true);
        testIsTimestampToTimestampWithTimeZoneInjectiveAt(ZoneId.of("Europe/Warsaw"), Instant.parse("2020-10-25T01:00:00.999999999Z"), true);
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
