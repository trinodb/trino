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
package io.trino.util;

import io.trino.spi.TrinoException;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.concurrent.Callable;

import static io.trino.sql.tree.IntervalLiteral.IntervalField.DAY;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.MINUTE;
import static io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND;
import static io.trino.util.DateTimeUtils.parseDayTimeInterval;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestDateTimeUtils
{
    @Test
    public void testParseDayTimeInterval()
    {
        assertThat(parseDayTimeInterval("2 5:10:1", DAY, Optional.of(SECOND)))
                .isEqualTo(millisIn(2, 5, 10, 1, 0));
        assertThat(parseDayTimeInterval("-2 5:10:1", DAY, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(2, 5, 10, 1, 0));
        assertThat(parseDayTimeInterval("5:10:1", HOUR, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 5, 10, 1, 0));
        assertThat(parseDayTimeInterval("-5:10:1", HOUR, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 5, 10, 1, 0));
        assertThat(parseDayTimeInterval("10:1", MINUTE, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 0, 10, 1, 0));
        assertThat(parseDayTimeInterval("-10:1", MINUTE, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 0, 10, 1, 0));
        assertThat(parseDayTimeInterval("1", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 0));
        assertThat(parseDayTimeInterval("-1", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 1, 0));

        assertThat(parseDayTimeInterval("1.", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 0));
        assertThat(parseDayTimeInterval("1,", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 0));
        assertThat(parseDayTimeInterval("-1.", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 1, 0));
        assertThat(parseDayTimeInterval("-1,", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 1, 0));

        assertThat(parseDayTimeInterval("2 5:10:1.123", DAY, Optional.of(SECOND)))
                .isEqualTo(millisIn(2, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("2 5:10:1,123", DAY, Optional.of(SECOND)))
                .isEqualTo(millisIn(2, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("-2 5:10:1.123", DAY, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(2, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("-2 5:10:1,123", DAY, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(2, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("5:10:1.123", HOUR, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("5:10:1,123", HOUR, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("-5:10:1.123", HOUR, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("-5:10:1,123", HOUR, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 5, 10, 1, 123));
        assertThat(parseDayTimeInterval("10:1.123", MINUTE, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 0, 10, 1, 123));
        assertThat(parseDayTimeInterval("10:1,123", MINUTE, Optional.of(SECOND)))
                .isEqualTo(millisIn(0, 0, 10, 1, 123));
        assertThat(parseDayTimeInterval("-10:1.123", MINUTE, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 0, 10, 1, 123));
        assertThat(parseDayTimeInterval("-10:1,123", MINUTE, Optional.of(SECOND)))
                .isEqualTo(-1 * millisIn(0, 0, 10, 1, 123));
        assertThat(parseDayTimeInterval("1.123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 123));
        assertThat(parseDayTimeInterval("1,123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 1, 123));
        assertThat(parseDayTimeInterval("-1.123", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 1, 123));
        assertThat(parseDayTimeInterval("-1,123", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 1, 123));
        assertThat(parseDayTimeInterval(".123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 0, 123));
        assertThat(parseDayTimeInterval(",123", SECOND, Optional.empty()))
                .isEqualTo(millisIn(0, 0, 0, 0, 123));
        assertThat(parseDayTimeInterval("-.123", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 0, 123));
        assertThat(parseDayTimeInterval("-,123", SECOND, Optional.empty()))
                .isEqualTo(-1 * millisIn(0, 0, 0, 0, 123));

        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("2 5:10:1.1234", DAY, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("2 5:10:1,1234", DAY, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-2 5:10:1.1234", DAY, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-2 5:10:1,1234", DAY, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("5:10:1.1234", HOUR, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("5:10:1,1234", HOUR, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-5:10:1.1234", HOUR, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-5:10:1,1234", HOUR, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("10:1.1234", MINUTE, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("10:1,1234", MINUTE, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-10:1.1234", MINUTE, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-10:1,1234", MINUTE, Optional.of(SECOND)));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("1.1234", SECOND, Optional.empty()));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("1,1234", SECOND, Optional.empty()));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-1.1234", SECOND, Optional.empty()));
        assertThrowsInvalidIntervalException(() -> parseDayTimeInterval("-1,1234", SECOND, Optional.empty()));
    }

    private long millisIn(long days, long hours, long minutes, long seconds, long millis)
    {
        return 86_400_000 * days
                + 3_600_000 * hours
                + 60_000 * minutes
                + 1_000 * seconds
                + millis;
    }

    private void assertThrowsInvalidIntervalException(Callable<Long> callable)
    {
        assertThatThrownBy(callable::call)
                .isInstanceOf(TrinoException.class)
                .hasMessageStartingWith("Invalid INTERVAL");
    }
}
