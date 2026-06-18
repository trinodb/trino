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
package io.trino.execution.admission;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestQueryMemoryHistory
{
    private static final DataSize FALLBACK = DataSize.of(7, MEGABYTE);

    @Test
    public void testColdStartReturnsFallback()
    {
        QueryMemoryHistory history = new QueryMemoryHistory(4);
        assertThat(history.sampleCount()).isZero();
        assertThat(history.averageOrDefault(FALLBACK)).isEqualTo(FALLBACK);
    }

    @Test
    public void testAveragesRecordedSamples()
    {
        QueryMemoryHistory history = new QueryMemoryHistory(4);
        history.record(DataSize.of(100, MEGABYTE).toBytes());
        history.record(DataSize.of(200, MEGABYTE).toBytes());

        assertThat(history.sampleCount()).isEqualTo(2);
        assertThat(history.averageOrDefault(FALLBACK).toBytes())
                .isEqualTo(DataSize.of(150, MEGABYTE).toBytes());
    }

    @Test
    public void testWindowEvictsOldestSample()
    {
        QueryMemoryHistory history = new QueryMemoryHistory(3);
        history.record(DataSize.of(30, MEGABYTE).toBytes());
        history.record(DataSize.of(60, MEGABYTE).toBytes());
        history.record(DataSize.of(90, MEGABYTE).toBytes());
        // average of 30, 60, 90 = 60
        assertThat(history.averageOrDefault(FALLBACK).toBytes())
                .isEqualTo(DataSize.of(60, MEGABYTE).toBytes());

        // overflow the window: 30 is evicted, leaving 60, 90, 120 -> 90
        history.record(DataSize.of(120, MEGABYTE).toBytes());
        assertThat(history.sampleCount()).isEqualTo(3);
        assertThat(history.averageOrDefault(FALLBACK).toBytes())
                .isEqualTo(DataSize.of(90, MEGABYTE).toBytes());
    }

    @Test
    public void testNegativeSampleClampedToZero()
    {
        QueryMemoryHistory history = new QueryMemoryHistory(2);
        history.record(-1);
        history.record(DataSize.of(100, MEGABYTE).toBytes());
        // (0 + 100MB) / 2 = 50MB
        assertThat(history.averageOrDefault(FALLBACK).toBytes())
                .isEqualTo(DataSize.of(50, MEGABYTE).toBytes());
    }

    @Test
    public void testRejectsNonPositiveWindow()
    {
        assertThatThrownBy(() -> new QueryMemoryHistory(0))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
