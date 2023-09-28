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
package io.trino.sql.planner.planprinter;

import io.trino.operator.WindowInfo;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.assertj.core.api.Assertions.assertThat;

public class TestWindowOperatorStats
{
    @Test
    public void testEmptyDriverInfosList()
    {
        WindowInfo info = new WindowInfo(emptyList());

        WindowOperatorStats stats = WindowOperatorStats.create(info);

        assertThat(stats.getIndexSizeStdDev()).isNaN();
        assertThat(stats.getIndexPositionsStdDev()).isNaN();
        assertThat(stats.getIndexCountPerDriverStdDev()).isNaN();
        assertThat(stats.getPartitionRowsStdDev()).isNaN();
        assertThat(stats.getRowsPerDriverStdDev()).isNaN();
        assertThat(stats.getActiveDrivers()).isEqualTo(0);
        assertThat(stats.getTotalDrivers()).isEqualTo(0);
    }
}
