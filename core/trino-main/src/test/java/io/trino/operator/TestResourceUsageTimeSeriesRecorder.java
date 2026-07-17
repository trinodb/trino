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
package io.trino.operator;

import io.airlift.testing.TestingTicker;
import io.trino.operator.ResourceUsageTimeSeriesRecorder.ResourceUsageTimeSeriesSnapshot;
import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static io.trino.operator.ResourceUsageTimeSeriesRecorder.ResourceUsageTimeSeriesSnapshot.EMPTY;
import static io.trino.operator.ResourceUsageTimeSeriesRecorder.merge;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestResourceUsageTimeSeriesRecorder
{
    @Test
    public void testBasicRecording()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);

        recorder.record(100, 50);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(1);
        assertThat(snapshot.cpuNanosBuckets()[0]).isEqualTo(50);
        assertThat(snapshot.wallNanosBuckets()[0]).isEqualTo(100);
    }

    @Test
    public void testRecordingInDifferentBuckets()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);

        recorder.record(100, 50);

        ticker.increment(1, SECONDS);
        recorder.record(200, 80);

        ticker.increment(1, SECONDS);
        recorder.record(300, 120);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(1);
        assertThat(snapshot.cpuNanosBuckets()).containsExactly(50, 80, 120);
        assertThat(snapshot.wallNanosBuckets()).containsExactly(100, 200, 300);
    }

    @Test
    public void testMultipleRecordsInSameBucket()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);

        recorder.record(100, 50);
        recorder.record(200, 80);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.cpuNanosBuckets()).containsExactly(130);
        assertThat(snapshot.wallNanosBuckets()).containsExactly(300);
    }

    @Test
    public void testBucketExpansion()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(4, ticker);

        recorder.record(100, 10);
        ticker.increment(1, SECONDS);
        recorder.record(200, 20);
        ticker.increment(1, SECONDS);
        recorder.record(300, 30);
        ticker.increment(1, SECONDS);
        recorder.record(400, 40);

        // Advance beyond capacity - triggers bucket width doubling
        ticker.increment(1, SECONDS);
        recorder.record(500, 50);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(2);
        // Buckets 0+1 merged, buckets 2+3 merged, then new record in bucket 2
        assertThat(snapshot.cpuNanosBuckets()).containsExactly(10 + 20, 30 + 40, 50);
        assertThat(snapshot.wallNanosBuckets()).containsExactly(100 + 200, 300 + 400, 500);
    }

    @Test
    public void testDoubleExpansion()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(4, ticker);

        for (int i = 0; i < 4; i++) {
            recorder.record(100, 10);
            if (i < 3) {
                ticker.increment(1, SECONDS);
            }
        }

        // Jump far enough to trigger double expansion
        ticker.increment(5, SECONDS);
        recorder.record(500, 50);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(4);
        assertThat(snapshot.cpuNanosBuckets()).containsExactly(40, 0, 50);
    }

    @Test
    public void testBucketExpansionWithStartNanosOffset()
    {
        // Ticker starts at 1.5S. Clock reports 0.5S into the second, so startNanos = 1.5S - 0.5S = 1.0S.
        // On expansion, bucketWidthNanos doubles to 2S, giving startNanosOffset = 1.0S % 2.0S = 1.0S > 0,
        // exercising the branch where the first old bucket is kept solo and subsequent pairs are merged.
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1500, MILLISECONDS);
        Clock clock = Clock.fixed(Instant.ofEpochSecond(1000, 500_000_000), ZoneOffset.UTC);
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(4, ticker, clock);

        recorder.record(100, 10);
        ticker.increment(1, SECONDS);
        recorder.record(200, 20);
        ticker.increment(1, SECONDS);
        recorder.record(300, 30);
        ticker.increment(1, SECONDS);
        recorder.record(400, 40);

        // Trigger expansion — with startNanosOffset > 0, the merge loop starts at
        // sourceOffset=1: bucket[0] is kept solo, buckets [1+2] are merged, and bucket[3] is
        // carried forward as an unpaired singleton into the next target slot.
        ticker.increment(1, SECONDS);
        recorder.record(500, 50);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isGreaterThanOrEqualTo(2);
        assertThat(snapshot.cpuNanosBuckets()).containsExactly(10, 50, 90);
    }

    @Test
    public void testExpansionPreservesAllRecordedData()
    {
        // Ticker starts at 1.5S so that startNanos = 1.5S - Instant.now().getNano() ∈ (0.5S, 1.5S).
        // This guarantees startNanosOffset > 0 on the first expansion, which causes the expansion
        // loop to start merging pairs from sourceOffset=1, leaving the last bucket (cpu[3]=10)
        // without a pair. The total CPU nanos must still equal the sum of all recorded values.
        TestingTicker ticker = new TestingTicker();
        ticker.increment(1500, MILLISECONDS);
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(4, ticker);

        long cpuPerRecord = 10;
        int recordCount = 5; // one more than bucket capacity to trigger exactly one expansion

        for (int i = 0; i < recordCount; i++) {
            recorder.record(cpuPerRecord, cpuPerRecord);
            if (i < recordCount - 1) {
                ticker.increment(1, SECONDS);
            }
        }

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        long totalCpuNanos = 0;
        for (long v : snapshot.cpuNanosBuckets()) {
            totalCpuNanos += v;
        }
        assertThat(totalCpuNanos).isEqualTo(cpuPerRecord * recordCount);
    }

    @Test
    public void testSnapshotStartTime()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);
        recorder.record(100, 10);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.startTimeEpochSeconds()).isGreaterThan(0);
    }

    @Test
    public void testEmptySnapshot()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.startTimeEpochSeconds()).isEqualTo(-1);
        assertThat(snapshot.isEmpty()).isTrue();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(1);
        assertThat(snapshot.cpuNanosBuckets()).isEmpty();
        assertThat(snapshot.wallNanosBuckets()).isEmpty();
    }

    @Test
    public void testMergeSameTimeline()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(ticker);

        recorder.record(100, 50);
        ticker.increment(1, SECONDS);
        recorder.record(200, 80);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        ResourceUsageTimeSeriesSnapshot merged = merge(snapshot, snapshot);

        assertThat(merged.bucketWidthSeconds()).isEqualTo(snapshot.bucketWidthSeconds());
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(snapshot.startTimeEpochSeconds());
        for (int i = 0; i < snapshot.cpuNanosBuckets().length; i++) {
            assertThat(merged.cpuNanosBuckets()[i]).isEqualTo(snapshot.cpuNanosBuckets()[i] * 2);
            assertThat(merged.wallNanosBuckets()[i]).isEqualTo(snapshot.wallNanosBuckets()[i] * 2);
        }
    }

    @Test
    public void testMergeWithDifferentBucketWidths()
    {
        long startTime = 1000;
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                startTime, 1, new long[] {10, 20, 30, 40}, new long[] {100, 200, 300, 400});
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                startTime, 2, new long[] {50, 60}, new long[] {700, 800});

        ResourceUsageTimeSeriesSnapshot merged = merge(narrow, wide);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.cpuNanosBuckets()).containsExactly(80, 130);
        assertThat(merged.wallNanosBuckets()).containsExactly(1000, 1500);
    }

    @Test
    public void testMergeWithDifferentStartTimes()
    {
        ResourceUsageTimeSeriesSnapshot first = ResourceUsageTimeSeriesSnapshot.create(
                100, 1, new long[] {10, 20}, new long[] {100, 200});
        ResourceUsageTimeSeriesSnapshot second = ResourceUsageTimeSeriesSnapshot.create(
                102, 1, new long[] {30, 40}, new long[] {300, 400});

        ResourceUsageTimeSeriesSnapshot merged = merge(first, second);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(100);
        assertThat(merged.cpuNanosBuckets()).containsExactly(10, 20, 30, 40);
    }

    @Test
    public void testMergeOverlappingTimelines()
    {
        ResourceUsageTimeSeriesSnapshot first = ResourceUsageTimeSeriesSnapshot.create(
                100, 1, new long[] {10, 20, 30, 40}, new long[] {100, 200, 300, 400});
        ResourceUsageTimeSeriesSnapshot second = ResourceUsageTimeSeriesSnapshot.create(
                102, 1, new long[] {50, 60}, new long[] {500, 600});

        ResourceUsageTimeSeriesSnapshot merged = merge(first, second);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(100);
        assertThat(merged.cpuNanosBuckets()).containsExactly(10, 20, 30 + 50, 40 + 60);
    }

    @Test
    public void testMergeNonOverlappingTimelines()
    {
        ResourceUsageTimeSeriesSnapshot first = ResourceUsageTimeSeriesSnapshot.create(
                10, 1, new long[] {10, 20}, new long[] {100, 200});
        ResourceUsageTimeSeriesSnapshot second = ResourceUsageTimeSeriesSnapshot.create(
                14, 1, new long[] {50, 60}, new long[] {500, 600});

        ResourceUsageTimeSeriesSnapshot merged = merge(first, second);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(10);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(1);
        assertThat(merged.cpuNanosBuckets()).containsExactly(10, 20, 0, 0, 50, 60);
    }

    @Test
    public void testMergeNonOverlappingTimelinesWithDifferentWidths()
    {
        ResourceUsageTimeSeriesSnapshot first = ResourceUsageTimeSeriesSnapshot.create(
                10, 1, new long[] {10, 20, 40}, new long[] {100, 200, 400});
        ResourceUsageTimeSeriesSnapshot second = ResourceUsageTimeSeriesSnapshot.create(
                16, 4, new long[] {50, 60}, new long[] {500, 600});

        ResourceUsageTimeSeriesSnapshot merged = merge(first, second);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(8);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(4);
        assertThat(merged.cpuNanosBuckets()).containsExactly(30, 40, 50, 60);
    }

    @Test
    public void testSnapshotInvalidBucketWidth()
    {
        assertThatThrownBy(() -> ResourceUsageTimeSeriesSnapshot.create(0, 0, new long[] {}, new long[] {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testSnapshotStartTimeMustBeTruncated()
    {
        assertThatThrownBy(() -> ResourceUsageTimeSeriesSnapshot.create(3, 2, new long[] {}, new long[] {}))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testMergeWithAdjustedStartTime()
    {
        ResourceUsageTimeSeriesSnapshot a = ResourceUsageTimeSeriesSnapshot.create(
                5, 1, new long[] {10, 20}, new long[] {100, 200});
        ResourceUsageTimeSeriesSnapshot b = ResourceUsageTimeSeriesSnapshot.create(
                6, 2, new long[] {50}, new long[] {500});

        ResourceUsageTimeSeriesSnapshot merged = merge(a, b);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(4);
    }

    @Test
    public void testMergeWithInitialBucketOffset()
    {
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                100, 2, new long[] {50, 60, 70, 80}, new long[] {500, 600, 700, 800});
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                103, 1, new long[] {10, 20}, new long[] {100, 200});

        ResourceUsageTimeSeriesSnapshot merged = merge(wide, narrow);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(100);
        assertThat(merged.cpuNanosBuckets()).hasSize(4);
        assertThat(merged.cpuNanosBuckets()).containsExactly(50, 60 + 10, 70 + 20, 80);
    }

    @Test
    public void testMergeThreeSnapshots()
    {
        ResourceUsageTimeSeriesSnapshot a = ResourceUsageTimeSeriesSnapshot.create(
                100, 1, new long[] {10, 20}, new long[] {100, 200});
        ResourceUsageTimeSeriesSnapshot b = ResourceUsageTimeSeriesSnapshot.create(
                100, 1, new long[] {30, 40}, new long[] {300, 400});
        ResourceUsageTimeSeriesSnapshot c = ResourceUsageTimeSeriesSnapshot.create(
                101, 1, new long[] {50}, new long[] {500});

        ResourceUsageTimeSeriesSnapshot merged = merge(a, b, c);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(100);
        assertThat(merged.cpuNanosBuckets()).hasSize(2);
        assertThat(merged.cpuNanosBuckets()).containsExactly(10 + 30, 20 + 40 + 50);
    }

    @Test
    public void testSnapshotAfterExpansionTruncatesStartTime()
    {
        TestingTicker ticker = new TestingTicker();
        ResourceUsageTimeSeriesRecorder recorder = new ResourceUsageTimeSeriesRecorder(4, ticker);

        // Fill and expand to get bucketWidthSeconds > 1
        for (int i = 0; i < 4; i++) {
            recorder.record(100, 10);
            ticker.increment(1, SECONDS);
        }
        recorder.record(100, 10);

        ResourceUsageTimeSeriesSnapshot snapshot = recorder.snapshot();
        assertThat(snapshot.bucketWidthSeconds()).isEqualTo(2);
        // Start time should be truncated to bucket width
        assertThat(snapshot.startTimeEpochSeconds() % snapshot.bucketWidthSeconds()).isZero();
    }

    @Test
    public void testMergeWithInitialBucketOffsetDoesNotDoubleCountPartialItems()
    {
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                100, 2, new long[] {50, 60, 70, 80}, new long[] {500, 600, 700, 800});
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                103, 1, new long[] {10, 20}, new long[] {100, 200});

        ResourceUsageTimeSeriesSnapshot merged = merge(wide, narrow);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(100);
        assertThat(merged.cpuNanosBuckets()).containsExactly(50, 60 + 10, 70 + 20, 80);
    }

    @Test
    public void testMergeWithUnadjustedEarliestStartTime()
    {
        ResourceUsageTimeSeriesSnapshot a = ResourceUsageTimeSeriesSnapshot.create(
                1, 1, new long[] {10, 20}, new long[] {100, 200});
        ResourceUsageTimeSeriesSnapshot b = ResourceUsageTimeSeriesSnapshot.create(
                2, 2, new long[] {50}, new long[] {500});

        ResourceUsageTimeSeriesSnapshot merged = merge(a, b);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(0);
        assertThat(merged.cpuNanosBuckets()).containsExactly(10, 70);
    }

    @Test
    public void testMergeWhenFineSnapshotExtendsOneBucketPastCoarseEnd()
    {
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                0, 2, new long[] {50}, new long[] {500});
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                0, 1, new long[] {10, 20, 30}, new long[] {100, 200, 300});

        ResourceUsageTimeSeriesSnapshot merged = merge(wide, narrow);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(2);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(0);
        assertThat(merged.cpuNanosBuckets()).containsExactly(80, 30);
        assertThat(merged.wallNanosBuckets()).containsExactly(800, 300);
    }

    @Test
    public void testMergeWithMisalignedFineSnapshotAndCoarseBucketWidth()
    {
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                0, 4, new long[] {50, 60}, new long[] {500, 600});
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                2, 2, new long[] {10, 20, 30}, new long[] {100, 200, 300});

        ResourceUsageTimeSeriesSnapshot merged = merge(wide, narrow);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(4);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(0);
        assertThat(merged.cpuNanosBuckets()).containsExactly(60, 110);
        assertThat(merged.wallNanosBuckets()).containsExactly(600, 1100);
    }

    @Test
    public void testMergeWithShortFineSnapshotInFirstPartialBucket()
    {
        ResourceUsageTimeSeriesSnapshot wide = ResourceUsageTimeSeriesSnapshot.create(
                0, 4, new long[] {50, 60, 70}, new long[] {500, 600, 700});
        ResourceUsageTimeSeriesSnapshot narrow = ResourceUsageTimeSeriesSnapshot.create(
                2, 1, new long[] {10}, new long[] {100});

        ResourceUsageTimeSeriesSnapshot merged = merge(wide, narrow);
        assertThat(merged.bucketWidthSeconds()).isEqualTo(4);
        assertThat(merged.startTimeEpochSeconds()).isEqualTo(0);
        assertThat(merged.cpuNanosBuckets()).containsExactly(60, 60, 70);
        assertThat(merged.wallNanosBuckets()).containsExactly(600, 600, 700);
    }

    @Test
    public void testMergeEmptySnapshots()
    {
        assertThat(merge(EMPTY, EMPTY)).isEqualTo(EMPTY);
        ResourceUsageTimeSeriesSnapshot nonEmpty = ResourceUsageTimeSeriesSnapshot.create(1, 1, new long[] {6}, new long[] {67});
        assertThat(merge(EMPTY, nonEmpty, EMPTY)).isEqualTo(nonEmpty);
    }
}
