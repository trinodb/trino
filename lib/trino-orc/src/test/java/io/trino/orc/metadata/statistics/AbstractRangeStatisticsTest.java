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
package io.trino.orc.metadata.statistics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public abstract class AbstractRangeStatisticsTest<R extends RangeStatistics<T>, T>
{
    protected abstract R getCreateStatistics(T min, T max);

    protected void assertMinMax(T min, T max)
    {
        assertMinMaxStatistics(min, min);
        assertMinMaxStatistics(max, max);
        assertMinMaxStatistics(min, max);
        assertMinMaxStatistics(min, null);
        assertMinMaxStatistics(null, max);

        if (!min.equals(max)) {
            assertThatThrownBy(() -> getCreateStatistics(max, min))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("minimum is not less than or equal to maximum");
        }
    }

    void assertRetainedSize(T min, T max, long expectedSizeInBytes)
    {
        assertThat(getCreateStatistics(min, max).getRetainedSizeInBytes()).isEqualTo(expectedSizeInBytes);
    }

    private void assertMinMaxStatistics(T min, T max)
    {
        R statistics = getCreateStatistics(min, max);
        assertThat(statistics.getMin()).isEqualTo(min);
        assertThat(statistics.getMax()).isEqualTo(max);

        assertThat(statistics).isEqualTo(statistics);
        assertThat(statistics.hashCode()).isEqualTo(statistics.hashCode());
    }
}
