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

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveTimestamp;
import static io.trino.type.DateTimes.MICROSECONDS_PER_MILLISECOND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHiveUtil
{
    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, DateTimeZone.UTC);
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss")).isEqualTo(unixTime(time, 0));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.S")).isEqualTo(unixTime(time, 1));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSS")).isEqualTo(unixTime(time, 3));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS")).isEqualTo(unixTime(time, 6));
        assertThat(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS")).isEqualTo(unixTime(time, 7));
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

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time));
    }

    private static long unixTime(DateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.getMillis() / factor) * factor * MICROSECONDS_PER_MILLISECOND;
    }

    public static DateTimeZone nonDefaultTimeZone()
    {
        String defaultId = DateTimeZone.getDefault().getID();
        for (String id : DateTimeZone.getAvailableIDs()) {
            if (!id.equals(defaultId)) {
                DateTimeZone zone = DateTimeZone.forID(id);
                if (zone.getStandardOffset(0) != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }
}
