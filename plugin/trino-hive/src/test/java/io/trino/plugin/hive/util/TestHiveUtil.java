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
package io.trino.plugin.hive.util;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveDate;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveTimestamp;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static java.time.ZoneOffset.UTC;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        ZonedDateTime time = ZonedDateTime.of(2011, 5, 6, 7, 8, 9, 123_000_000, UTC);
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss")).isEqualTo(unixTime(time, 0));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.S")).isEqualTo(unixTime(time, 1));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSS")).isEqualTo(unixTime(time, 3));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS")).isEqualTo(unixTime(time, 6));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")).isEqualTo(unixTime(time, 7));
    }

    @Test
    public void testPartitionTimestampFormats()
    {
        assertThat(parseHiveTimestamp("+2000-1-2 3:4:5.123456789")).isEqualTo(946_782_245_123_000L);
        assertThat(parseHiveTimestamp("1969-12-31 23:59:59.999999999")).isEqualTo(-1_000L);
        assertThat(parseHiveDate("-0001-1-1")).isEqualTo(LocalDate.of(-1, 1, 1).toEpochDay());
        assertThat(parseHiveDate("2000-2-29 0:0:0.000000001")).isEqualTo(LocalDate.of(2000, 2, 29).toEpochDay());
        assertThatThrownBy(() -> parseHiveTimestamp("2023-2-29")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseHiveTimestamp("2020-1-1 24:0:0")).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> parseHiveDate("2020-1-1 0:0:0.001")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testEscapeDatabaseName()
    {
        assertThat(escapeSchemaName("schema1")).isEqualTo("schema1");
        assertThatThrownBy(() -> escapeSchemaName(null))
                .hasMessage("The provided schemaName cannot be null or empty");
        assertThatThrownBy(() -> escapeSchemaName(""))
                .hasMessage("The provided schemaName cannot be null or empty");
        assertThat(escapeSchemaName("../schema1")).isEqualTo("..%2Fschema1");
        assertThat(escapeSchemaName("../../schema1")).isEqualTo("..%2F..%2Fschema1");
    }

    @Test
    public void testEscapeTableName()
    {
        assertThat(escapeTableName("table1")).isEqualTo("table1");
        assertThatThrownBy(() -> escapeTableName(null))
                .hasMessage("The provided tableName cannot be null or empty");
        assertThatThrownBy(() -> escapeTableName(""))
                .hasMessage("The provided tableName cannot be null or empty");
        assertThat(escapeTableName("../table1")).isEqualTo("..%2Ftable1");
        assertThat(escapeTableName("../../table1")).isEqualTo("..%2F..%2Ftable1");
    }

    private static long parse(ZonedDateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormatter.ofPattern(pattern).format(time));
    }

    private static long unixTime(ZonedDateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.toInstant().toEpochMilli() / factor) * factor * MICROSECONDS_PER_MILLISECOND;
    }

    public static ZoneId nonDefaultTimeZone()
    {
        String defaultId = ZoneId.systemDefault().getId();
        for (String id : ZoneId.getAvailableZoneIds()) {
            if (!id.equals(defaultId)) {
                ZoneId zone = ZoneId.of(id);
                if (zone.getRules().getStandardOffset(Instant.EPOCH).getTotalSeconds() != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }
}
