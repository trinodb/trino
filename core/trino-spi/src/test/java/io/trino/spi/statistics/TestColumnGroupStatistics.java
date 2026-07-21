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
package io.trino.spi.statistics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestColumnGroupStatistics
{
    @Test
    public void testEmpty()
    {
        ColumnGroupStatistics empty = ColumnGroupStatistics.empty();
        assertThat(empty.getDistinctValuesCount().isUnknown()).isTrue();
    }

    @Test
    public void testBuilderWithKnownNdv()
    {
        ColumnGroupStatistics stats = ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(42))
                .build();
        assertThat(stats.getDistinctValuesCount().getValue()).isEqualTo(42.0);
    }

    @Test
    public void testBuilderRejectsNegativeNdv()
    {
        assertThatThrownBy(() -> ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(-1))
                .build())
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("distinctValuesCount must be greater than or equal to 0");
    }

    @Test
    public void testEqualsAndHashCode()
    {
        ColumnGroupStatistics a = ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(100))
                .build();
        ColumnGroupStatistics b = ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(100))
                .build();
        ColumnGroupStatistics c = ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(200))
                .build();

        assertThat(a).isEqualTo(b);
        assertThat(a.hashCode()).isEqualTo(b.hashCode());
        assertThat(a).isNotEqualTo(c);
    }

    @Test
    public void testToString()
    {
        ColumnGroupStatistics stats = ColumnGroupStatistics.builder()
                .setDistinctValuesCount(Estimate.of(77))
                .build();
        assertThat(stats.toString()).contains("77");
    }
}
