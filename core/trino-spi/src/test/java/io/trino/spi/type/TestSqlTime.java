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
package io.trino.spi.type;

import org.junit.jupiter.api.Test;

import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_DAY;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlTime
{
    @Test
    public void testToString()
    {
        assertThat(SqlTime.newInstance(0, 0).toString()).isEqualTo("00:00:00");
        assertThat(SqlTime.newInstance(1, 0).toString()).isEqualTo("00:00:00.0");
        assertThat(SqlTime.newInstance(2, 0).toString()).isEqualTo("00:00:00.00");
        assertThat(SqlTime.newInstance(3, 0).toString()).isEqualTo("00:00:00.000");
        assertThat(SqlTime.newInstance(4, 0).toString()).isEqualTo("00:00:00.0000");
        assertThat(SqlTime.newInstance(5, 0).toString()).isEqualTo("00:00:00.00000");
        assertThat(SqlTime.newInstance(6, 0).toString()).isEqualTo("00:00:00.000000");
        assertThat(SqlTime.newInstance(7, 0).toString()).isEqualTo("00:00:00.0000000");
        assertThat(SqlTime.newInstance(8, 0).toString()).isEqualTo("00:00:00.00000000");
        assertThat(SqlTime.newInstance(9, 0).toString()).isEqualTo("00:00:00.000000000");
        assertThat(SqlTime.newInstance(10, 0).toString()).isEqualTo("00:00:00.0000000000");
        assertThat(SqlTime.newInstance(11, 0).toString()).isEqualTo("00:00:00.00000000000");
        assertThat(SqlTime.newInstance(12, 0).toString()).isEqualTo("00:00:00.000000000000");
    }

    @Test
    public void testRoundTo()
    {
        // round down
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(0)).isEqualTo(SqlTime.newInstance(0, 0));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(1)).isEqualTo(SqlTime.newInstance(1, 100000000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(2)).isEqualTo(SqlTime.newInstance(2, 110000000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(3)).isEqualTo(SqlTime.newInstance(3, 111000000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(4)).isEqualTo(SqlTime.newInstance(4, 111100000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(5)).isEqualTo(SqlTime.newInstance(5, 111110000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(6)).isEqualTo(SqlTime.newInstance(6, 111111000000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(7)).isEqualTo(SqlTime.newInstance(7, 111111100000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(8)).isEqualTo(SqlTime.newInstance(8, 111111110000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(9)).isEqualTo(SqlTime.newInstance(9, 111111111000L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(10)).isEqualTo(SqlTime.newInstance(10, 111111111100L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(11)).isEqualTo(SqlTime.newInstance(11, 111111111110L));
        assertThat(SqlTime.newInstance(12, 111111111111L).roundTo(12)).isEqualTo(SqlTime.newInstance(12, 111111111111L));

        // round up
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(0)).isEqualTo(SqlTime.newInstance(0, 1000000000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(1)).isEqualTo(SqlTime.newInstance(1, 600000000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(2)).isEqualTo(SqlTime.newInstance(2, 560000000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(3)).isEqualTo(SqlTime.newInstance(3, 556000000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(4)).isEqualTo(SqlTime.newInstance(4, 555600000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(5)).isEqualTo(SqlTime.newInstance(5, 555560000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(6)).isEqualTo(SqlTime.newInstance(6, 555556000000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(7)).isEqualTo(SqlTime.newInstance(7, 555555600000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(8)).isEqualTo(SqlTime.newInstance(8, 555555560000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(9)).isEqualTo(SqlTime.newInstance(9, 555555556000L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(10)).isEqualTo(SqlTime.newInstance(10, 555555555600L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(11)).isEqualTo(SqlTime.newInstance(11, 555555555560L));
        assertThat(SqlTime.newInstance(12, 555555555555L).roundTo(12)).isEqualTo(SqlTime.newInstance(12, 555555555555L));

        // round up to next day
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(0)).isEqualTo(SqlTime.newInstance(0, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(1)).isEqualTo(SqlTime.newInstance(1, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(2)).isEqualTo(SqlTime.newInstance(2, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(3)).isEqualTo(SqlTime.newInstance(3, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(4)).isEqualTo(SqlTime.newInstance(4, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(5)).isEqualTo(SqlTime.newInstance(5, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(6)).isEqualTo(SqlTime.newInstance(6, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(7)).isEqualTo(SqlTime.newInstance(7, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(8)).isEqualTo(SqlTime.newInstance(8, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(9)).isEqualTo(SqlTime.newInstance(9, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(10)).isEqualTo(SqlTime.newInstance(10, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(11)).isEqualTo(SqlTime.newInstance(11, 0));
        assertThat(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L).roundTo(12)).isEqualTo(SqlTime.newInstance(12, PICOSECONDS_PER_DAY - 1L));
    }
}
