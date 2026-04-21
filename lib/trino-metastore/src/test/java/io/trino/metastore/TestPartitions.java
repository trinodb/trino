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
package io.trino.metastore;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.junit.jupiter.api.Test;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;

import static io.trino.metastore.Partitions.toPartitionValues;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartitions
{
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

    private static void assertToPartitionValues(String partitionName)
            throws MetaException
    {
        List<String> actual = toPartitionValues(partitionName);
        AbstractList<String> expected = new ArrayList<>();
        actual.forEach(s -> expected.add(null));
        Warehouse.makeValsFromName(partitionName, expected);
        assertThat(actual).isEqualTo(expected);
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
        assertThat(Partitions.unescapePathName(value)).isEqualTo(expected);
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
        assertThat(Partitions.escapePathName(value)).isEqualTo(expected);
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
        assertThat(Partitions.makePartName(columns, values)).isEqualTo(expected);
    }
}
