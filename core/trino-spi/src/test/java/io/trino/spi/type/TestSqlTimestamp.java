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

import org.testng.annotations.Test;

import java.time.LocalDateTime;

import static io.trino.spi.type.SqlTimestamp.fromMillis;
import static io.trino.spi.type.SqlTimestamp.newInstance;
import static org.assertj.core.api.Assertions.assertThat;

public class TestSqlTimestamp
{
    @Test
    public void testBaseline()
    {
        assertThat(newInstance(0, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00");
        assertThat(newInstance(1, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.0");
        assertThat(newInstance(2, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.00");
        assertThat(newInstance(3, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.000");
        assertThat(newInstance(4, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.0000");
        assertThat(newInstance(5, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.00000");
        assertThat(newInstance(6, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.000000");
        assertThat(newInstance(7, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.0000000");
        assertThat(newInstance(8, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.00000000");
        assertThat(newInstance(9, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.000000000");
        assertThat(newInstance(10, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.0000000000");
        assertThat(newInstance(11, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.00000000000");
        assertThat(newInstance(12, 0, 0).toString()).isEqualTo("1970-01-01 00:00:00.000000000000");
    }

    @Test
    public void testFromMillis()
    {
        // round down, positive epoch
        // represents a timestamp of 1970-01-01 00:00:00.111
        assertThat(fromMillis(0, 111).toString()).isEqualTo("1970-01-01 00:00:00");
        assertThat(fromMillis(1, 111).toString()).isEqualTo("1970-01-01 00:00:00.1");
        assertThat(fromMillis(2, 111).toString()).isEqualTo("1970-01-01 00:00:00.11");
        assertThat(fromMillis(3, 111).toString()).isEqualTo("1970-01-01 00:00:00.111");
        assertThat(fromMillis(4, 111).toString()).isEqualTo("1970-01-01 00:00:00.1110");
        assertThat(fromMillis(5, 111).toString()).isEqualTo("1970-01-01 00:00:00.11100");
        assertThat(fromMillis(6, 111).toString()).isEqualTo("1970-01-01 00:00:00.111000");
        assertThat(fromMillis(7, 111).toString()).isEqualTo("1970-01-01 00:00:00.1110000");
        assertThat(fromMillis(8, 111).toString()).isEqualTo("1970-01-01 00:00:00.11100000");
        assertThat(fromMillis(9, 111).toString()).isEqualTo("1970-01-01 00:00:00.111000000");
        assertThat(fromMillis(10, 111).toString()).isEqualTo("1970-01-01 00:00:00.1110000000");
        assertThat(fromMillis(11, 111).toString()).isEqualTo("1970-01-01 00:00:00.11100000000");
        assertThat(fromMillis(12, 111).toString()).isEqualTo("1970-01-01 00:00:00.111000000000");

        // round up, positive epoch
        // represents a timestamp of 1970-01-01 00:00:00.555
        assertThat(fromMillis(0, 555).toString()).isEqualTo("1970-01-01 00:00:01");
        assertThat(fromMillis(1, 555).toString()).isEqualTo("1970-01-01 00:00:00.6");
        assertThat(fromMillis(2, 555).toString()).isEqualTo("1970-01-01 00:00:00.56");
        assertThat(fromMillis(3, 555).toString()).isEqualTo("1970-01-01 00:00:00.555");
        assertThat(fromMillis(4, 555).toString()).isEqualTo("1970-01-01 00:00:00.5550");
        assertThat(fromMillis(5, 555).toString()).isEqualTo("1970-01-01 00:00:00.55500");
        assertThat(fromMillis(6, 555).toString()).isEqualTo("1970-01-01 00:00:00.555000");
        assertThat(fromMillis(7, 555).toString()).isEqualTo("1970-01-01 00:00:00.5550000");
        assertThat(fromMillis(8, 555).toString()).isEqualTo("1970-01-01 00:00:00.55500000");
        assertThat(fromMillis(9, 555).toString()).isEqualTo("1970-01-01 00:00:00.555000000");
        assertThat(fromMillis(10, 555).toString()).isEqualTo("1970-01-01 00:00:00.5550000000");
        assertThat(fromMillis(11, 555).toString()).isEqualTo("1970-01-01 00:00:00.55500000000");
        assertThat(fromMillis(12, 555).toString()).isEqualTo("1970-01-01 00:00:00.555000000000");

        // round down, negative epoch
        // represents a timestamp of 1969-12-31 23:59:59.111
        assertThat(fromMillis(0, -889).toString()).isEqualTo("1969-12-31 23:59:59");
        assertThat(fromMillis(1, -889).toString()).isEqualTo("1969-12-31 23:59:59.1");
        assertThat(fromMillis(2, -889).toString()).isEqualTo("1969-12-31 23:59:59.11");
        assertThat(fromMillis(3, -889).toString()).isEqualTo("1969-12-31 23:59:59.111");
        assertThat(fromMillis(4, -889).toString()).isEqualTo("1969-12-31 23:59:59.1110");
        assertThat(fromMillis(5, -889).toString()).isEqualTo("1969-12-31 23:59:59.11100");
        assertThat(fromMillis(6, -889).toString()).isEqualTo("1969-12-31 23:59:59.111000");
        assertThat(fromMillis(7, -889).toString()).isEqualTo("1969-12-31 23:59:59.1110000");
        assertThat(fromMillis(8, -889).toString()).isEqualTo("1969-12-31 23:59:59.11100000");
        assertThat(fromMillis(9, -889).toString()).isEqualTo("1969-12-31 23:59:59.111000000");
        assertThat(fromMillis(10, -889).toString()).isEqualTo("1969-12-31 23:59:59.1110000000");
        assertThat(fromMillis(11, -889).toString()).isEqualTo("1969-12-31 23:59:59.11100000000");
        assertThat(fromMillis(12, -889).toString()).isEqualTo("1969-12-31 23:59:59.111000000000");

        // round up, negative epoch
        // represents a timestamp of 1969-12-31 23:59:59.555
        assertThat(fromMillis(0, -445).toString()).isEqualTo("1970-01-01 00:00:00");
        assertThat(fromMillis(1, -445).toString()).isEqualTo("1969-12-31 23:59:59.6");
        assertThat(fromMillis(2, -445).toString()).isEqualTo("1969-12-31 23:59:59.56");
        assertThat(fromMillis(3, -445).toString()).isEqualTo("1969-12-31 23:59:59.555");
        assertThat(fromMillis(4, -445).toString()).isEqualTo("1969-12-31 23:59:59.5550");
        assertThat(fromMillis(5, -445).toString()).isEqualTo("1969-12-31 23:59:59.55500");
        assertThat(fromMillis(6, -445).toString()).isEqualTo("1969-12-31 23:59:59.555000");
        assertThat(fromMillis(7, -445).toString()).isEqualTo("1969-12-31 23:59:59.5550000");
        assertThat(fromMillis(8, -445).toString()).isEqualTo("1969-12-31 23:59:59.55500000");
        assertThat(fromMillis(9, -445).toString()).isEqualTo("1969-12-31 23:59:59.555000000");
        assertThat(fromMillis(10, -445).toString()).isEqualTo("1969-12-31 23:59:59.5550000000");
        assertThat(fromMillis(11, -445).toString()).isEqualTo("1969-12-31 23:59:59.55500000000");
        assertThat(fromMillis(12, -445).toString()).isEqualTo("1969-12-31 23:59:59.555000000000");
    }

    @Test
    public void testRoundTo()
    {
        // positive epoch, round down
        assertThat(newInstance(12, 111111, 111111).roundTo(0).toString()).isEqualTo("1970-01-01 00:00:00");
        assertThat(newInstance(12, 111111, 111111).roundTo(1).toString()).isEqualTo("1970-01-01 00:00:00.1");
        assertThat(newInstance(12, 111111, 111111).roundTo(2).toString()).isEqualTo("1970-01-01 00:00:00.11");
        assertThat(newInstance(12, 111111, 111111).roundTo(3).toString()).isEqualTo("1970-01-01 00:00:00.111");
        assertThat(newInstance(12, 111111, 111111).roundTo(4).toString()).isEqualTo("1970-01-01 00:00:00.1111");
        assertThat(newInstance(12, 111111, 111111).roundTo(5).toString()).isEqualTo("1970-01-01 00:00:00.11111");
        assertThat(newInstance(12, 111111, 111111).roundTo(6).toString()).isEqualTo("1970-01-01 00:00:00.111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(7).toString()).isEqualTo("1970-01-01 00:00:00.1111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(8).toString()).isEqualTo("1970-01-01 00:00:00.11111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(9).toString()).isEqualTo("1970-01-01 00:00:00.111111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(10).toString()).isEqualTo("1970-01-01 00:00:00.1111111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(11).toString()).isEqualTo("1970-01-01 00:00:00.11111111111");
        assertThat(newInstance(12, 111111, 111111).roundTo(12).toString()).isEqualTo("1970-01-01 00:00:00.111111111111");

        // positive epoch, round up
        assertThat(newInstance(12, 555555, 555555).roundTo(0).toString()).isEqualTo("1970-01-01 00:00:01");
        assertThat(newInstance(12, 555555, 555555).roundTo(1).toString()).isEqualTo("1970-01-01 00:00:00.6");
        assertThat(newInstance(12, 555555, 555555).roundTo(2).toString()).isEqualTo("1970-01-01 00:00:00.56");
        assertThat(newInstance(12, 555555, 555555).roundTo(3).toString()).isEqualTo("1970-01-01 00:00:00.556");
        assertThat(newInstance(12, 555555, 555555).roundTo(4).toString()).isEqualTo("1970-01-01 00:00:00.5556");
        assertThat(newInstance(12, 555555, 555555).roundTo(5).toString()).isEqualTo("1970-01-01 00:00:00.55556");
        assertThat(newInstance(12, 555555, 555555).roundTo(6).toString()).isEqualTo("1970-01-01 00:00:00.555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(7).toString()).isEqualTo("1970-01-01 00:00:00.5555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(8).toString()).isEqualTo("1970-01-01 00:00:00.55555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(9).toString()).isEqualTo("1970-01-01 00:00:00.555555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(10).toString()).isEqualTo("1970-01-01 00:00:00.5555555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(11).toString()).isEqualTo("1970-01-01 00:00:00.55555555556");
        assertThat(newInstance(12, 555555, 555555).roundTo(12).toString()).isEqualTo("1970-01-01 00:00:00.555555555555");

        // negative epoch, round down
        // represents a timestamp of 1969-12-31 23:59:59.111111111111
        assertThat(newInstance(12, -888889, 111111).roundTo(0).toString()).isEqualTo("1969-12-31 23:59:59");
        assertThat(newInstance(12, -888889, 111111).roundTo(1).toString()).isEqualTo("1969-12-31 23:59:59.1");
        assertThat(newInstance(12, -888889, 111111).roundTo(2).toString()).isEqualTo("1969-12-31 23:59:59.11");
        assertThat(newInstance(12, -888889, 111111).roundTo(3).toString()).isEqualTo("1969-12-31 23:59:59.111");
        assertThat(newInstance(12, -888889, 111111).roundTo(4).toString()).isEqualTo("1969-12-31 23:59:59.1111");
        assertThat(newInstance(12, -888889, 111111).roundTo(5).toString()).isEqualTo("1969-12-31 23:59:59.11111");
        assertThat(newInstance(12, -888889, 111111).roundTo(6).toString()).isEqualTo("1969-12-31 23:59:59.111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(7).toString()).isEqualTo("1969-12-31 23:59:59.1111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(8).toString()).isEqualTo("1969-12-31 23:59:59.11111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(9).toString()).isEqualTo("1969-12-31 23:59:59.111111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(10).toString()).isEqualTo("1969-12-31 23:59:59.1111111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(11).toString()).isEqualTo("1969-12-31 23:59:59.11111111111");
        assertThat(newInstance(12, -888889, 111111).roundTo(12).toString()).isEqualTo("1969-12-31 23:59:59.111111111111");

        // negative epoch, round up
        // represents a timestamp of 1969-12-31 23:59:59.555555555555
        assertThat(newInstance(12, -444445, 555555).roundTo(0).toString()).isEqualTo("1970-01-01 00:00:00");
        assertThat(newInstance(12, -444445, 555555).roundTo(1).toString()).isEqualTo("1969-12-31 23:59:59.6");
        assertThat(newInstance(12, -444445, 555555).roundTo(2).toString()).isEqualTo("1969-12-31 23:59:59.56");
        assertThat(newInstance(12, -444445, 555555).roundTo(3).toString()).isEqualTo("1969-12-31 23:59:59.556");
        assertThat(newInstance(12, -444445, 555555).roundTo(4).toString()).isEqualTo("1969-12-31 23:59:59.5556");
        assertThat(newInstance(12, -444445, 555555).roundTo(5).toString()).isEqualTo("1969-12-31 23:59:59.55556");
        assertThat(newInstance(12, -444445, 555555).roundTo(6).toString()).isEqualTo("1969-12-31 23:59:59.555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(7).toString()).isEqualTo("1969-12-31 23:59:59.5555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(8).toString()).isEqualTo("1969-12-31 23:59:59.55555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(9).toString()).isEqualTo("1969-12-31 23:59:59.555555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(10).toString()).isEqualTo("1969-12-31 23:59:59.5555555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(11).toString()).isEqualTo("1969-12-31 23:59:59.55555555556");
        assertThat(newInstance(12, -444445, 555555).roundTo(12).toString()).isEqualTo("1969-12-31 23:59:59.555555555555");
    }

    @Test
    public void testToLocalDateTime()
    {
        SqlTimestamp timestamp = newInstance(9, 1367846055987654L, 321_000);
        assertThat(timestamp.toString()).isEqualTo("2013-05-06 13:14:15.987654321");
        assertThat(timestamp.toLocalDateTime()).isEqualTo(LocalDateTime.of(2013, 5, 6, 13, 14, 15, 987_654_321));

        timestamp = newInstance(9, -178454744012346L, 321_000);
        assertThat(timestamp.toString()).isEqualTo("1964-05-06 13:14:15.987654321");
        assertThat(timestamp.toLocalDateTime()).isEqualTo(LocalDateTime.of(1964, 5, 6, 13, 14, 15, 987_654_321));

        timestamp = newInstance(12, 555_555, 555_555);
        assertThat(timestamp.toString()).isEqualTo("1970-01-01 00:00:00.555555555555");
        assertThat(timestamp.toLocalDateTime()).isEqualTo(LocalDateTime.of(1970, 1, 1, 0, 0, 0, 555_555_556));

        timestamp = newInstance(12, -444_445, 555_555);
        assertThat(timestamp.toString()).isEqualTo("1969-12-31 23:59:59.555555555555");
        assertThat(timestamp.toLocalDateTime()).isEqualTo(LocalDateTime.of(1969, 12, 31, 23, 59, 59, 555_555_556));
    }
}
