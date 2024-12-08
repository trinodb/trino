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
package io.trino.sql.gen;

import io.trino.sql.gen.columnar.FilterReorderingProfiler;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

final class TestFilterReorderingProfiler
{
    @Test
    void testReorderingDisabled()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, false);
        profiler.addFilterMetrics(2, 100, 10_000);
        profiler.addFilterMetrics(1, 100, 1_000);
        profiler.addFilterMetrics(0, 100, 100);

        profiler.reorderFilters(10_000);
        assertThat(profiler.getFilterOrder()).containsExactly(0, 1, 2);
    }

    @Test
    void testNoReordering()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, true);
        profiler.addFilterMetrics(0, 100, 10_000);
        profiler.addFilterMetrics(1, 100, 1_000);
        profiler.addFilterMetrics(2, 100, 100);

        profiler.reorderFilters(10_000);
        assertThat(profiler.getFilterOrder()).containsExactly(0, 1, 2);
    }

    @Test
    void testReorderingBySelectivity()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, true);
        profiler.addFilterMetrics(2, 100, 10_000);
        profiler.addFilterMetrics(1, 100, 1_000);
        profiler.addFilterMetrics(0, 100, 100);

        profiler.reorderFilters(10_000);
        assertThat(profiler.getFilterOrder()).containsExactly(2, 1, 0);
    }

    @Test
    void testReorderingByTimeTaken()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, true);
        profiler.addFilterMetrics(0, 1000, 10_000);
        profiler.addFilterMetrics(1, 100, 10_000);
        profiler.addFilterMetrics(2, 10, 10_000);

        profiler.reorderFilters(10_000);
        assertThat(profiler.getFilterOrder()).containsExactly(2, 1, 0);
    }

    @Test
    void testReorderingByTimeTakenPerFilteredPosition()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, true);
        profiler.addFilterMetrics(0, 1000, 8_000);
        profiler.addFilterMetrics(1, 700, 6_000);
        profiler.addFilterMetrics(2, 600, 4_000);

        profiler.reorderFilters(8_000);
        assertThat(profiler.getFilterOrder()).containsExactly(1, 0, 2);
    }

    @Test
    void testMinSampleSize()
    {
        FilterReorderingProfiler profiler = new FilterReorderingProfiler(3, true);
        profiler.addFilterMetrics(0, 1000, 800);
        profiler.addFilterMetrics(1, 700, 600);
        profiler.addFilterMetrics(2, 600, 400);

        profiler.reorderFilters(800);
        assertThat(profiler.getFilterOrder()).containsExactly(0, 1, 2);

        profiler.addFilterMetrics(0, 1000, 800);
        profiler.addFilterMetrics(1, 700, 600);
        profiler.addFilterMetrics(2, 600, 400);

        profiler.reorderFilters(800);
        assertThat(profiler.getFilterOrder()).containsExactly(0, 1, 2);

        profiler.addFilterMetrics(0, 1000, 1_000);
        profiler.addFilterMetrics(1, 700, 2_000);
        profiler.addFilterMetrics(2, 600, 3_000);

        profiler.reorderFilters(3_000);
        assertThat(profiler.getFilterOrder()).containsExactly(2, 1, 0);
    }
}
