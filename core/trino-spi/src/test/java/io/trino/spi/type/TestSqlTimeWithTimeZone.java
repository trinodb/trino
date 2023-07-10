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

public class TestSqlTimeWithTimeZone
{
    @Test
    public void testToString()
    {
        assertThat(SqlTimeWithTimeZone.newInstance(0, 0, 1).toString()).isEqualTo("00:00:00+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(1, 0, 1).toString()).isEqualTo("00:00:00.0+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(2, 0, 1).toString()).isEqualTo("00:00:00.00+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(3, 0, 1).toString()).isEqualTo("00:00:00.000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(4, 0, 1).toString()).isEqualTo("00:00:00.0000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(5, 0, 1).toString()).isEqualTo("00:00:00.00000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(6, 0, 1).toString()).isEqualTo("00:00:00.000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(7, 0, 1).toString()).isEqualTo("00:00:00.0000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(8, 0, 1).toString()).isEqualTo("00:00:00.00000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(9, 0, 1).toString()).isEqualTo("00:00:00.000000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(10, 0, 1).toString()).isEqualTo("00:00:00.0000000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(11, 0, 1).toString()).isEqualTo("00:00:00.00000000000+00:01");
        assertThat(SqlTimeWithTimeZone.newInstance(12, 0, 1).toString()).isEqualTo("00:00:00.000000000000+00:01");
    }

    @Test
    public void testEqualsDifferentZone()
    {
        assertThat(SqlTimeWithTimeZone.newInstance(12, 0, 0))
                .isNotEqualTo(SqlTimeWithTimeZone.newInstance(12, 0, 1));
    }

    @Test
    public void testRoundTo()
    {
        // round down
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(0)).isEqualTo(SqlTimeWithTimeZone.newInstance(0, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(1)).isEqualTo(SqlTimeWithTimeZone.newInstance(1, 100000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(2)).isEqualTo(SqlTimeWithTimeZone.newInstance(2, 110000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(3)).isEqualTo(SqlTimeWithTimeZone.newInstance(3, 111000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(4)).isEqualTo(SqlTimeWithTimeZone.newInstance(4, 111100000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(5)).isEqualTo(SqlTimeWithTimeZone.newInstance(5, 111110000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(6)).isEqualTo(SqlTimeWithTimeZone.newInstance(6, 111111000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(7)).isEqualTo(SqlTimeWithTimeZone.newInstance(7, 111111100000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(8)).isEqualTo(SqlTimeWithTimeZone.newInstance(8, 111111110000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(9)).isEqualTo(SqlTimeWithTimeZone.newInstance(9, 111111111000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(10)).isEqualTo(SqlTimeWithTimeZone.newInstance(10, 111111111100L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(11)).isEqualTo(SqlTimeWithTimeZone.newInstance(11, 111111111110L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1).roundTo(12)).isEqualTo(SqlTimeWithTimeZone.newInstance(12, 111111111111L, 1));

        // round up
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(0)).isEqualTo(SqlTimeWithTimeZone.newInstance(0, 1000000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(1)).isEqualTo(SqlTimeWithTimeZone.newInstance(1, 600000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(2)).isEqualTo(SqlTimeWithTimeZone.newInstance(2, 560000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(3)).isEqualTo(SqlTimeWithTimeZone.newInstance(3, 556000000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(4)).isEqualTo(SqlTimeWithTimeZone.newInstance(4, 555600000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(5)).isEqualTo(SqlTimeWithTimeZone.newInstance(5, 555560000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(6)).isEqualTo(SqlTimeWithTimeZone.newInstance(6, 555556000000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(7)).isEqualTo(SqlTimeWithTimeZone.newInstance(7, 555555600000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(8)).isEqualTo(SqlTimeWithTimeZone.newInstance(8, 555555560000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(9)).isEqualTo(SqlTimeWithTimeZone.newInstance(9, 555555556000L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(10)).isEqualTo(SqlTimeWithTimeZone.newInstance(10, 555555555600L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(11)).isEqualTo(SqlTimeWithTimeZone.newInstance(11, 555555555560L, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1).roundTo(12)).isEqualTo(SqlTimeWithTimeZone.newInstance(12, 555555555555L, 1));

        // round up to next day
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(0)).isEqualTo(SqlTimeWithTimeZone.newInstance(0, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(1)).isEqualTo(SqlTimeWithTimeZone.newInstance(1, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(2)).isEqualTo(SqlTimeWithTimeZone.newInstance(2, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(3)).isEqualTo(SqlTimeWithTimeZone.newInstance(3, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(4)).isEqualTo(SqlTimeWithTimeZone.newInstance(4, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(5)).isEqualTo(SqlTimeWithTimeZone.newInstance(5, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(6)).isEqualTo(SqlTimeWithTimeZone.newInstance(6, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(7)).isEqualTo(SqlTimeWithTimeZone.newInstance(7, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(8)).isEqualTo(SqlTimeWithTimeZone.newInstance(8, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(9)).isEqualTo(SqlTimeWithTimeZone.newInstance(9, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(10)).isEqualTo(SqlTimeWithTimeZone.newInstance(10, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(11)).isEqualTo(SqlTimeWithTimeZone.newInstance(11, 0, 1));
        assertThat(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1).roundTo(12)).isEqualTo(SqlTimeWithTimeZone.newInstance(12, PICOSECONDS_PER_DAY - 1L, 1));
    }
}
