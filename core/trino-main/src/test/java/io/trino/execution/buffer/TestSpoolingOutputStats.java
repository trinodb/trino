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
package io.trino.execution.buffer;

import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.data.Percentage.withPercentage;
import static org.testng.Assert.assertEquals;

public class TestSpoolingOutputStats
{
    // The algorithm discards 15 least significant bits of mantissa out of 23 retaining 8 most significant bits
    private static final double EXPECTED_PRECISION_LOSS_IN_PERCENTS = 100.0 / 256;

    @Test
    public void test()
    {
        int numberOfPartitions = 15;
        SpoolingOutputStats spoolingOutputStats = new SpoolingOutputStats(numberOfPartitions);
        assertThat(spoolingOutputStats.getFinalSnapshot()).isEmpty();
        long[] expectedValues = new long[numberOfPartitions];
        long value = 1;
        for (int partition = 0; partition < numberOfPartitions; partition++) {
            spoolingOutputStats.update(partition, value);
            expectedValues[partition] = value;
            value *= 31;
        }
        assertThat(spoolingOutputStats.getFinalSnapshot()).isEmpty();
        spoolingOutputStats.finish();
        assertThat(spoolingOutputStats.getFinalSnapshot()).isPresent();

        // update is allowed to be called after finish, the invocation is ignored
        spoolingOutputStats.update(0, value);
        // finish is allowed to be called multiple times, the invocation is ignored
        spoolingOutputStats.finish();
        assertThat(spoolingOutputStats.getFinalSnapshot()).isPresent();

        SpoolingOutputStats.Snapshot snapshot = spoolingOutputStats.getFinalSnapshot().orElseThrow();
        assertEquals(snapshot.getPartitionSizeInBytes(0), 1);

        for (int partition = 0; partition < numberOfPartitions; partition++) {
            assertThat(snapshot.getPartitionSizeInBytes(partition)).isCloseTo(expectedValues[partition], withPercentage(EXPECTED_PRECISION_LOSS_IN_PERCENTS));
        }
    }
}
