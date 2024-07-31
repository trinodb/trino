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
package io.trino.hive.formats;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

import static io.trino.hive.formats.HiveFormatUtils.TIMESTAMP_FORMATS_KEY;
import static io.trino.hive.formats.HiveFormatUtils.getTimestampFormatsSchemaProperty;
import static io.trino.hive.formats.HiveFormatUtils.parseHiveDate;
import static io.trino.hive.formats.HiveFormatsErrorCode.HIVE_INVALID_METADATA;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHiveFormatUtils
{
    @Test
    public void test()
    {
        assertThat(parseHiveDate("5881580-07-11")).isEqualTo(LocalDate.of(5881580, 7, 11));
        assertThat(parseHiveDate("+5881580-07-11")).isEqualTo(LocalDate.of(5881580, 7, 11));
        assertThat(parseHiveDate("-5877641-06-23")).isEqualTo(LocalDate.of(-5877641, 6, 23));
        assertThat(parseHiveDate("1986-01-33")).isEqualTo(LocalDate.of(1986, 2, 2));
    }

    @Test
    public void testTimestampFormatEscaping()
    {
        assertTrinoExceptionThrownBy(() -> getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\")))
                .hasErrorCode(HIVE_INVALID_METADATA)
                .hasMessageContaining("unterminated escape");
        assertTrinoExceptionThrownBy(() -> getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\neither backslash nor comma")))
                .hasErrorCode(HIVE_INVALID_METADATA)
                .hasMessageContaining("Illegal escaped character");
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\\\"))).isEqualTo(ImmutableList.of("\\"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "xx\\\\"))).isEqualTo(ImmutableList.of("xx\\"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\\\yy"))).isEqualTo(ImmutableList.of("\\yy"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "xx\\\\yy"))).isEqualTo(ImmutableList.of("xx\\yy"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\,"))).isEqualTo(ImmutableList.of(","));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "aa\\,"))).isEqualTo(ImmutableList.of("aa,"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "\\,bb"))).isEqualTo(ImmutableList.of(",bb"));
        assertThat(getTimestampFormatsSchemaProperty(ImmutableMap.of(TIMESTAMP_FORMATS_KEY, "aa\\,bb"))).isEqualTo(ImmutableList.of("aa,bb"));
    }
}
