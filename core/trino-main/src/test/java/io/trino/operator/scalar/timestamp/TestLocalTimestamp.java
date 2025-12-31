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
package io.trino.operator.scalar.timestamp;

import io.trino.spi.type.LongTimestamp;
import io.trino.testing.TestingSession;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.ZoneOffset;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.spi.type.TimestampType.MAX_SHORT_PRECISION;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static java.lang.Math.addExact;
import static java.lang.Math.toIntExact;
import static org.assertj.core.api.Assertions.assertThat;

class TestLocalTimestamp
{
    @Test
    void testLocalTimestamp()
    {
        long baseSeconds = 1126351860;

        long baseZonedMicros = Instant.ofEpochSecond(baseSeconds)
                .atZone(TestingSession.DEFAULT_TIME_ZONE_KEY.getZoneId())
                .toLocalDateTime()
                .atZone(ZoneOffset.UTC)
                .toInstant()
                .getEpochSecond() * 1_000_000;

        LongTimestamp baseZonedLong = new LongTimestamp(baseZonedMicros, 0);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 0, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 1, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 2, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 3, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 4, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 5, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 6, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 7, baseZonedLong);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 8, baseZonedLong);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 9, baseZonedLong);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 10, baseZonedLong);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 11, baseZonedLong);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 0), 12, baseZonedLong);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 0, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 1, baseZonedMicros + 100_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 2, baseZonedMicros + 110_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 3, baseZonedMicros + 111_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 4, baseZonedMicros + 111_100);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 5, baseZonedMicros + 111_110);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 6, baseZonedMicros + 111_111);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 7, addNanos(baseZonedLong, 111_111_100));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 8, addNanos(baseZonedLong, 111_111_110));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 9, addNanos(baseZonedLong, 111_111_111));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 10, addNanos(baseZonedLong, 111_111_111));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 11, addNanos(baseZonedLong, 111_111_111));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_111), 12, addNanos(baseZonedLong, 111_111_111));

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 0, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 1, baseZonedMicros + 400_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 2, baseZonedMicros + 440_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 3, baseZonedMicros + 444_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 4, baseZonedMicros + 444_400);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 5, baseZonedMicros + 444_440);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 6, baseZonedMicros + 444_444);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 7, addNanos(baseZonedLong, 444_444_400));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 8, addNanos(baseZonedLong, 444_444_440));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 9, addNanos(baseZonedLong, 444_444_444));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 10, addNanos(baseZonedLong, 444_444_444));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 11, addNanos(baseZonedLong, 444_444_444));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 444_444_444), 12, addNanos(baseZonedLong, 444_444_444));

        // Rounding cutoffs
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 499_999_999), 0, baseZonedMicros);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 500_000_000), 0, baseZonedMicros + 1_000_000);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 149_999_999), 1, baseZonedMicros + 100_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 150_000_000), 1, baseZonedMicros + 200_000);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 114_999_999), 2, baseZonedMicros + 110_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 115_000_000), 2, baseZonedMicros + 120_000);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_499_999), 3, baseZonedMicros + 111_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_500_000), 3, baseZonedMicros + 112_000);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_149_999), 4, baseZonedMicros + 111_100);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_150_000), 4, baseZonedMicros + 111_200);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_114_999), 5, baseZonedMicros + 111_110);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_115_000), 5, baseZonedMicros + 111_120);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_499), 6, baseZonedMicros + 111_111);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_500), 6, baseZonedMicros + 111_112);

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_149), 7, addNanos(baseZonedLong, 111_111_100));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_150), 7, addNanos(baseZonedLong, 111_111_200));

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_114), 8, addNanos(baseZonedLong, 111_111_110));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 111_111_115), 8, addNanos(baseZonedLong, 111_111_120));

        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 0, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 1, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 2, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 3, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 4, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 5, baseZonedMicros + 1_000_000);
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 6, baseZonedMicros + 1_000_000);
        // TODO (https://github.com/trinodb/trino/issues/27807) this currently fails
//        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 7, addNanos(baseZonedLong, 1_000_000_000));
//        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 8, addNanos(baseZonedLong, 1_000_000_000));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 9, addNanos(baseZonedLong, 999_999_999));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 10, addNanos(baseZonedLong, 999_999_999));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 11, addNanos(baseZonedLong, 999_999_999));
        assertLocalTimestamp(Instant.ofEpochSecond(baseSeconds, 999_999_999), 12, addNanos(baseZonedLong, 999_999_999));
    }

    private static void assertLocalTimestamp(Instant sessionStartTime, int precision, Object expected)
    {
        var session = testSessionBuilder()
                .setStart(sessionStartTime)
                .build()
                .toConnectorSession();
        var result = (precision <= MAX_SHORT_PRECISION)
                ? LocalTimestamp.localTimestamp(precision, session, (Long) null)
                : LocalTimestamp.localTimestamp(precision, session, (LongTimestamp) null);
        assertThat(result).isEqualTo(expected);
    }

    private Object addNanos(LongTimestamp baseZonedLong, int nanosToAdd)
    {
        checkArgument(0 <= nanosToAdd, "nanosToAdd must be non-negative: %s", nanosToAdd);
        int microsToAdd = nanosToAdd / 1_000;
        int picosOfMicroToAdd = nanosToAdd % 1_000 * 1_000;
        return new LongTimestamp(
                baseZonedLong.getEpochMicros() + microsToAdd,
                toIntExact(addExact(baseZonedLong.getPicosOfMicro(), picosOfMicroToAdd)));
    }
}
