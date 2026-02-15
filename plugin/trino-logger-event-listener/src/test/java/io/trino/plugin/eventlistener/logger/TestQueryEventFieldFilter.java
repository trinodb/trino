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
package io.trino.plugin.eventlistener.logger;

import io.airlift.units.DataSize;
import org.junit.jupiter.api.Test;

import java.util.Set;

import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestQueryEventFieldFilter
{
    @Test
    public void testTruncateStringWithinLimit()
    {
        String value = "SELECT * FROM table";
        String result = QueryEventFieldFilter.truncateString(value, 100);
        assertThat(result).isEqualTo(value);
    }

    @Test
    public void testTruncateStringExceedsLimit()
    {
        String value = "SELECT * FROM very_very_very_long_table_name";
        String result = QueryEventFieldFilter.truncateString(value, 20);
        assertThat(result).contains("...[TRUNCATED]");
        assertThat(result.length()).isLessThanOrEqualTo(35); // Approximate bounds
    }

    @Test
    public void testTruncateStringNull()
    {
        String result = QueryEventFieldFilter.truncateString(null, 20);
        assertThat(result).isNull();
    }

    @Test
    public void testTruncateStringZeroLimit()
    {
        String value = "test string";
        String result = QueryEventFieldFilter.truncateString(value, 0);
        assertThat(result).isEqualTo(value);
    }

    @Test
    public void testTruncateStringNegativeLimit()
    {
        String value = "test string";
        String result = QueryEventFieldFilter.truncateString(value, -1);
        assertThat(result).isEqualTo(value);
    }

    @Test
    public void testTruncateMultibyteCharacters()
    {
        String value = "SELECT * FROM table WHERE emoji = '😀😀😀😀😀'";
        String result = QueryEventFieldFilter.truncateString(value, 30);
        assertThat(result).contains("...[TRUNCATED]");
        // Ensure byte length is within limit
        assertThat(result.getBytes().length).isLessThanOrEqualTo(40);
    }

    @Test
    public void testFilterInitialization()
    {
        QueryEventFieldFilter filter = new QueryEventFieldFilter(
                Set.of("user", "password"),
                DataSize.of(4, KILOBYTE),
                Set.of("query"),
                DataSize.of(2, KILOBYTE));

        // Just verify it initializes without errors
        assertThat(filter).isNotNull();
    }

    @Test
    public void testFilterWithEmptyExclusions()
    {
        QueryEventFieldFilter filter = new QueryEventFieldFilter(
                Set.of(),
                DataSize.of(4, KILOBYTE),
                Set.of(),
                DataSize.of(2, KILOBYTE));

        assertThat(filter).isNotNull();
    }

    @Test
    public void testFilterWithLargeDataSize()
    {
        QueryEventFieldFilter filter = new QueryEventFieldFilter(
                Set.of("field1"),
                DataSize.of(100, KILOBYTE),
                Set.of("field2"),
                DataSize.of(50, KILOBYTE));

        assertThat(filter).isNotNull();
    }
}
