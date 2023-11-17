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
package io.trino.spiller;

import io.airlift.units.DataSize;
import io.trino.ExceededSpillLimitException;
import org.junit.jupiter.api.Test;

import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestSpillSpaceTracker
{
    private static final DataSize MAX_DATA_SIZE = DataSize.of(10, MEGABYTE);

    @Test
    public void testSpillSpaceTracker()
    {
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(MAX_DATA_SIZE);

        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(0);
        assertThat(spillSpaceTracker.getMaxBytes()).isEqualTo(MAX_DATA_SIZE.toBytes());
        long reservedBytes = DataSize.of(5, MEGABYTE).toBytes();
        spillSpaceTracker.reserve(reservedBytes);
        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(reservedBytes);

        long otherReservedBytes = DataSize.of(2, MEGABYTE).toBytes();
        spillSpaceTracker.reserve(otherReservedBytes);
        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo((reservedBytes + otherReservedBytes));

        spillSpaceTracker.reserve(otherReservedBytes);
        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo((reservedBytes + 2 * otherReservedBytes));

        spillSpaceTracker.free(otherReservedBytes);
        spillSpaceTracker.free(otherReservedBytes);
        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(reservedBytes);

        spillSpaceTracker.free(reservedBytes);
        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(0);
    }

    @Test
    public void testSpillOutOfSpace()
    {
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(MAX_DATA_SIZE);

        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(0);
        assertThatThrownBy(() -> spillSpaceTracker.reserve(MAX_DATA_SIZE.toBytes() + 1))
                .isInstanceOf(ExceededSpillLimitException.class)
                .hasMessageMatching("Query exceeded local spill limit of.*");
    }

    @Test
    public void testFreeToMuch()
    {
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(MAX_DATA_SIZE);

        assertThat(spillSpaceTracker.getCurrentBytes()).isEqualTo(0);
        spillSpaceTracker.reserve(1000);
        assertThatThrownBy(() -> spillSpaceTracker.free(1001))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("tried to free more disk space than is reserved");
    }
}
