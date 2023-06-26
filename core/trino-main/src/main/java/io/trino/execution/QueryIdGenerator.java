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
package io.trino.execution;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Chars;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import io.trino.spi.QueryId;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ThreadLocalRandom;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class QueryIdGenerator
{
    // a-z, 0-9, except: l, o, 0, 1
    private static final char[] BASE_32 = {
            'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h',
            'i', 'j', 'k', 'm', 'n', 'p', 'q', 'r',
            's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
            '2', '3', '4', '5', '6', '7', '8', '9'};

    static {
        checkState(BASE_32.length == 32);
        checkState(ImmutableSet.copyOf(Chars.asList(BASE_32)).size() == 32);
    }

    private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss").withZone(UTC);
    private static final long BASE_SYSTEM_TIME_MILLIS = System.currentTimeMillis();
    private static final long BASE_NANO_TIME = System.nanoTime();

    private final String coordinatorId;
    @GuardedBy("this")
    private long lastTimeInDays;
    @GuardedBy("this")
    private long lastTimeInSeconds;
    @GuardedBy("this")
    private String lastTimestamp;
    @GuardedBy("this")
    private int counter;

    public QueryIdGenerator()
    {
        StringBuilder coordinatorId = new StringBuilder(5);
        for (int i = 0; i < 5; i++) {
            coordinatorId.append(BASE_32[ThreadLocalRandom.current().nextInt(32)]);
        }
        this.coordinatorId = coordinatorId.toString();
    }

    public String getCoordinatorId()
    {
        return coordinatorId;
    }

    /**
     * Generate next queryId using the following format:
     * {@code YYYYMMDD_hhmmss_index_coordId}
     * <p/>
     * {@code index} rolls at the start of every day or when it is close to reaching {@code 99,999}.
     * {@code coordId} is a randomly generated when this instance is created.
     */
    public synchronized QueryId createNextQueryId()
    {
        long now = nowInMillis();

        // restart counter if it is at the limit
        if (counter > 99_999) {
            // Wait for the timestamp to change to reset the counter to avoid duplicates
            // in the very unlikely case we generated all 100,000 in the previous second
            while (MILLISECONDS.toSeconds(now) == lastTimeInSeconds) {
                long delta = SECONDS.toMillis(lastTimeInSeconds + 1) - now;
                sleepUninterruptibly(delta, MILLISECONDS);
                now = nowInMillis();
            }
            counter = 0;
        }

        // if the second changed since the last ID was generated, generate a new timestamp
        if (MILLISECONDS.toSeconds(now) != lastTimeInSeconds) {
            // generate new timestamp
            lastTimeInSeconds = MILLISECONDS.toSeconds(now);
            lastTimestamp = formatEpochMilli(now);

            // if the day has rolled over, restart the counter
            if (MILLISECONDS.toDays(now) != lastTimeInDays) {
                lastTimeInDays = MILLISECONDS.toDays(now);
                counter = 0;
            }

            // restart the counter if it is close to the limit, to avoid sleep on rollover
            if (counter >= 90_000) {
                counter = 0;
            }
        }

        long index = counter;
        counter++;

        return new QueryId(format("%s_%05d_%s", lastTimestamp, index, coordinatorId));
    }

    private static String formatEpochMilli(long milli)
    {
        return TIMESTAMP_FORMAT.format(Instant.ofEpochMilli(milli));
    }

    @VisibleForTesting
    protected long nowInMillis()
    {
        // avoid problems with the clock moving backwards
        return BASE_SYSTEM_TIME_MILLIS + NANOSECONDS.toMillis(System.nanoTime() - BASE_NANO_TIME);
    }
}
