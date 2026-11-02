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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlTaskIoStats
{
    @Test
    public void testUpdateRecordsGrowth()
    {
        SqlTaskIoStats stats = new SqlTaskIoStats();
        stats.update(new SqlTaskIoTotals(100, 10, 50, 5));
        stats.update(new SqlTaskIoTotals(300, 30, 80, 8));

        assertTotals(stats, new SqlTaskIoTotals(300, 30, 80, 8));
        assertThat(stats.getInputDataSize().getOneMinute().getCount()).isPositive();
    }

    @Test
    public void testUpdateIgnoresTransientDecrease()
    {
        SqlTaskIoStats stats = new SqlTaskIoStats();
        stats.update(new SqlTaskIoTotals(100, 10, 50, 5));
        stats.update(new SqlTaskIoTotals(40, 4, 20, 2));
        assertTotals(stats, new SqlTaskIoTotals(100, 10, 50, 5));

        stats.update(new SqlTaskIoTotals(120, 12, 60, 6));
        assertTotals(stats, new SqlTaskIoTotals(120, 12, 60, 6));
    }

    private static void assertTotals(SqlTaskIoStats stats, SqlTaskIoTotals expected)
    {
        assertThat(stats.getInputDataSize().getTotalCount()).isEqualTo(expected.inputDataSize());
        assertThat(stats.getInputPositions().getTotalCount()).isEqualTo(expected.inputPositions());
        assertThat(stats.getOutputDataSize().getTotalCount()).isEqualTo(expected.outputDataSize());
        assertThat(stats.getOutputPositions().getTotalCount()).isEqualTo(expected.outputPositions());
    }
}
