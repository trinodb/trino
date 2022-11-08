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
}
