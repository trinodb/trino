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

import io.trino.spi.QueryId;
import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestQueryIdGenerator
{
    @Test
    public void testCreateNextQueryId()
    {
        TestIdGenerator idGenerator = new TestIdGenerator();

        long millis = epochMillis(2001, 7, 14, 1, 2, 3, 4);
        idGenerator.setNow(millis);

        // generate ids to 99,999
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010714_010203_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // next id will cause counter to roll, but we need to add a second to the time or code will block for ever
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010714_010204_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // move forward one more second and generate 100 ids
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 0; i < 100; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010714_010205_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // move forward one more second and verify counter not reset
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 100; i < 200; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010714_010206_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // now we move to the start of the next day, and the counter should reset
        millis = epochMillis(2001, 7, 15, 0, 0, 0, 0);
        idGenerator.setNow(millis);
        for (int i = 0; i < 90_123; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010715_000000_%05d_%s", i, idGenerator.getCoordinatorId())));
        }

        // moving forward one second with counter close to the limit causes it to roll
        millis += 1000;
        idGenerator.setNow(millis);
        for (int i = 0; i < 100_000; i++) {
            assertEquals(idGenerator.createNextQueryId(), new QueryId(format("20010715_000001_%05d_%s", i, idGenerator.getCoordinatorId())));
        }
    }

    private static long epochMillis(int year, int month, int day, int hour, int minute, int second, int milli)
    {
        int nano = toIntExact(MILLISECONDS.toNanos(milli));
        LocalDateTime dateTime = LocalDateTime.of(year, month, day, hour, minute, second, nano);
        return dateTime.toInstant(UTC).toEpochMilli();
    }

    private static class TestIdGenerator
            extends QueryIdGenerator
    {
        private long now;

        public void setNow(long now)
        {
            this.now = now;
        }

        @Override
        protected long nowInMillis()
        {
            return now;
        }
    }
}
