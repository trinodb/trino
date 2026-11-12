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
package io.trino.plugin.kafka;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestRange
{
    @Test
    void testPartition()
    {
        assertThat(new Range(0, 12).partition(5))
                .containsExactly(
                        new Range(0, 5),
                        new Range(5, 10),
                        new Range(10, 12));
    }

    @Test
    void testPartitionWithNonZeroBegin()
    {
        assertThat(new Range(5, 12).partition(5))
                .containsExactly(
                        new Range(5, 10),
                        new Range(10, 12));
    }

    @Test
    void testPartitionWithExactBoundary()
    {
        assertThat(new Range(0, 10).partition(5))
                .containsExactly(
                        new Range(0, 5),
                        new Range(5, 10));
    }

    @Test
    void testPartitionWithPartitionSizeLargerThanRange()
    {
        assertThat(new Range(5, 8).partition(10))
                .containsExactly(new Range(5, 8));
    }

    @Test
    void testPartitionOfEmptyRange()
    {
        assertThat(new Range(5, 5).partition(10)).isEmpty();
    }

    @Test
    void testPartitionOfInvertedRange()
    {
        assertThat(new Range(10, 5).partition(3)).isEmpty();
    }

    @Test
    void testPartitionRejectsNonPositivePartitionSize()
    {
        for (int partitionSize : new int[] {0, -1}) {
            assertThatThrownBy(() -> new Range(0, 10).partition(partitionSize))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessage("partitionSize must be greater than zero");
        }
    }
}
