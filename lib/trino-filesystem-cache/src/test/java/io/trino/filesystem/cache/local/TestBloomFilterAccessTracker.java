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
package io.trino.filesystem.cache.local;

import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;

import static org.assertj.core.api.Assertions.assertThat;

final class TestBloomFilterAccessTracker
{
    @Test
    void testRotatedBucketsSurviveRestart(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingClock clock = new TestingClock();
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        BloomFilterAccessTracker tracker = new BloomFilterAccessTracker(cacheRoot, Duration.valueOf("10m"), Duration.valueOf("1m"), DataSize.valueOf("1MB"), clock, stats);

        tracker.record("first");
        clock.advanceMillis(61_000);
        tracker.record("second");

        BloomFilterAccessTracker restarted = new BloomFilterAccessTracker(cacheRoot, Duration.valueOf("10m"), Duration.valueOf("1m"), DataSize.valueOf("1MB"), clock, new NativeFileSystemCacheStats());

        assertThat(restarted.mightContain("first")).isTrue();
        assertThat(restarted.lastAccessTime("first")).hasValue(0);
        assertThat(restarted.mightContain("unknown")).isFalse();
        assertThat(restarted.lastAccessTime("unknown")).isEmpty();
        assertThat(stats.getAccessRecords()).isEqualTo(2);
    }

    @Test
    void testLastAccessTimeUsesNewestMatchingBucket(@TempDir Path cacheRoot)
            throws Exception
    {
        TestingClock clock = new TestingClock();
        NativeFileSystemCacheStats stats = new NativeFileSystemCacheStats();
        BloomFilterAccessTracker tracker = new BloomFilterAccessTracker(cacheRoot, Duration.valueOf("10m"), Duration.valueOf("1m"), DataSize.valueOf("1MB"), clock, stats);

        tracker.record("file");
        clock.advanceMillis(61_000);
        tracker.record("other");
        clock.advanceMillis(61_000);
        tracker.record("file");

        assertThat(tracker.lastAccessTime("file")).hasValue(120_000);
    }

    private static class TestingClock
            extends Clock
    {
        private long millis;

        void advanceMillis(long millis)
        {
            this.millis += millis;
        }

        @Override
        public ZoneId getZone()
        {
            return ZoneId.of("UTC");
        }

        @Override
        public Clock withZone(ZoneId zone)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Instant instant()
        {
            return Instant.ofEpochMilli(millis);
        }

        @Override
        public long millis()
        {
            return millis;
        }
    }
}
