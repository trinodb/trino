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

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static io.trino.plugin.hive.util.HiveUtil.escapeSchemaName;
import static io.trino.plugin.hive.util.HiveUtil.escapeTableName;
import static io.trino.plugin.hive.util.HiveUtil.parseHiveTimestamp;
import static io.trino.plugin.hive.util.HiveUtil.toPartitionValues;
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
    public void testToPartitionValues()
            throws MetaException
    {
        assertToPartitionValues("ds=2015-12-30/event_type=QueryCompletion");
        assertToPartitionValues("ds=2015-12-30");
        assertToPartitionValues("a=1/b=2/c=3");
        assertToPartitionValues("a=1");
        assertToPartitionValues("pk=!@%23$%25%5E&%2A()%2F%3D");
        assertToPartitionValues("pk=__HIVE_DEFAULT_PARTITION__");
    }

    @Test
    public void testUnescapePathName()
    {
        assertUnescapePathName("", "");
        assertUnescapePathName("x", "x");
        assertUnescapePathName("abc", "abc");
        assertUnescapePathName("abc%", "abc%");
        assertUnescapePathName("%", "%");
        assertUnescapePathName("%41", "A");
        assertUnescapePathName("%41%x", "A%x");
        assertUnescapePathName("%41%xxZ", "A%xxZ");
        assertUnescapePathName("%41%%Z", "A%%Z");
        assertUnescapePathName("%41%25%25Z", "A%%Z");
        assertUnescapePathName("abc%41%42%43", "abcABC");
        assertUnescapePathName("abc%3Axyz", "abc:xyz");
        assertUnescapePathName("abc%3axyz", "abc:xyz");
        assertUnescapePathName("abc%BBxyz", "abc\u00BBxyz");
    }

    private static void assertUnescapePathName(String value, String expected)
    {
        assertThat(FileUtils.unescapePathName(value)).isEqualTo(expected);
        assertThat(HiveUtil.unescapePathName(value)).isEqualTo(expected);
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

    @Test
    public void testEscapePathName()
    {
        assertEscapePathName(null, "__HIVE_DEFAULT_PARTITION__");
        assertEscapePathName("", "__HIVE_DEFAULT_PARTITION__");
        assertEscapePathName("x", "x");
        assertEscapePathName("abc", "abc");
        assertEscapePathName("%", "%25");
        assertEscapePathName("A", "A");
        assertEscapePathName("A%x", "A%25x");
        assertEscapePathName("A%xxZ", "A%25xxZ");
        assertEscapePathName("A%%Z", "A%25%25Z");
        assertEscapePathName("abcABC", "abcABC");
        assertEscapePathName("abc:xyz", "abc%3Axyz");
        assertEscapePathName("abc\u00BBxyz", "abc\u00BBxyz");
        assertEscapePathName("\u0000\t\b\r\n\u001F", "%00%09%08%0D%0A%1F");
        assertEscapePathName("#%^&*=[]{\\:'\"/?", "%23%25%5E&%2A%3D%5B%5D%7B%5C%3A%27%22%2F%3F");
        assertEscapePathName("~`!@$()-_+}|;,.<>", "~`!@$()-_+}|;,.<>");
    }

    private static void assertEscapePathName(String value, String expected)
    {
        assertThat(FileUtils.escapePathName(value)).isEqualTo(expected);
        assertThat(HiveUtil.escapePathName(value)).isEqualTo(expected);
    }

    @Test
    public void testMakePartName()
    {
        assertMakePartName(List.of("abc"), List.of("xyz"), "abc=xyz");
        assertMakePartName(List.of("abc:qqq"), List.of("xyz/yyy=zzz"), "abc%3Aqqq=xyz%2Fyyy%3Dzzz");
        assertMakePartName(List.of("abc", "def", "xyz"), List.of("qqq", "rrr", "sss"), "abc=qqq/def=rrr/xyz=sss");
    }

    private static void assertMakePartName(List<String> columns, List<String> values, String expected)
    {
        assertThat(FileUtils.makePartName(columns, values)).isEqualTo(expected);
        assertThat(HiveUtil.makePartName(columns, values)).isEqualTo(expected);
    }

    private static void assertToPartitionValues(String partitionName)
            throws MetaException
    {
        List<String> actual = toPartitionValues(partitionName);
        AbstractList<String> expected = new ArrayList<>();
        actual.forEach(s -> expected.add(null));
        Warehouse.makeValsFromName(partitionName, expected);
        assertThat(actual).isEqualTo(expected);
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
