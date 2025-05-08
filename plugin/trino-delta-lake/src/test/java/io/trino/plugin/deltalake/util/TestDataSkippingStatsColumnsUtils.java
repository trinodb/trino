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
package io.trino.plugin.deltalake.util;

import com.google.common.collect.ImmutableSet;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.plugin.deltalake.util.DataSkippingStatsColumnsUtils.getDataSkippingStatsColumns;
import static io.trino.plugin.deltalake.util.DataSkippingStatsColumnsUtils.toDataSkippingStatsColumnsString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

final class TestDataSkippingStatsColumnsUtils
{
    @Test
    void testSkippingStatsColumns()
    {
        assertThat(getDataSkippingStatsColumns(Optional.empty())).isEmpty();

        assertThat(getDataSkippingStatsColumns(Optional.of("a,b,c"))).isEqualTo(ImmutableSet.of("a", "b", "c"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("a", "b", "c"))).isEqualTo("a,b,c");

        assertThat(getDataSkippingStatsColumns(Optional.of("aaa,bbb,cc"))).isEqualTo(ImmutableSet.of("aaa", "bbb", "cc"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("aaa", "bbb", "cc"))).isEqualTo("aaa,bbb,cc");

        assertThat(getDataSkippingStatsColumns(Optional.of("`a!b`, `a#b`, `a$b`, `a-b`"))).isEqualTo(ImmutableSet.of("a\\!b", "a\\#b", "a\\$b", "a\\-b"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("a\\!b", "a\\#b", "a\\$b", "a\\-b"))).isEqualTo("`a!b`,`a#b`,`a$b`,`a-b`");

        assertThat(getDataSkippingStatsColumns(Optional.of("`a@!#b`, `a[]%.#b`, `a$%^&b`"))).isEqualTo(ImmutableSet.of("a\\@\\!\\#b", "a\\[\\]\\%\\.\\#b", "a\\$\\%\\^\\&b"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("a\\@\\!\\#b", "a\\[\\]\\%\\.\\#b", "a\\$\\%\\^\\&b"))).isEqualTo("`a@!#b`,`a[]%.#b`,`a$%^&b`");

        assertThat(getDataSkippingStatsColumns(Optional.of("`a.b.c`, `aa.b.c`, `a\\.b.c`, `a,b,c`, `a``b`")))
                .isEqualTo(ImmutableSet.of("a\\.b\\.c", "aa\\.b\\.c", "a\\\\\\.b\\.c", "a\\,b\\,c", "a`b"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("a\\.b\\.c", "aa\\.b\\.c", "a\\\\\\.b\\.c", "a\\,b\\,c", "a`b")))
                .isEqualTo("`a.b.c`,`aa.b.c`,`a\\.b.c`,`a,b,c`,`a``b`");

        assertThat(getDataSkippingStatsColumns(Optional.of("a.b,`a.b`,`a\\.b.c`,abc,`a,b,c`")))
                .isEqualTo(ImmutableSet.of("a.b", "a\\.b", "a\\\\\\.b\\.c", "abc", "a\\,b\\,c"));
        assertThat(toDataSkippingStatsColumnsString(ImmutableSet.of("a.b", "a\\.b", "a\\\\\\.b\\.c", "abc", "a\\,b\\,c")))
                .isEqualTo("a.b,`a.b`,`a\\.b.c`,abc,`a,b,c`");

        assertThatCode(() -> getDataSkippingStatsColumns(Optional.of("abc,a$b")))
                .hasMessage("Invalid name in delta.dataSkippingStatsColumns property: a$b");
        assertThatCode(() -> getDataSkippingStatsColumns(Optional.of("abc,a\\!#b")))
                .hasMessage("Invalid name in delta.dataSkippingStatsColumns property: a\\!#b");

        assertThatCode(() -> getDataSkippingStatsColumns(Optional.of("`ab")))
                .hasMessage("Invalid value for delta.dataSkippingStatsColumns property: `ab");
        assertThatCode(() -> getDataSkippingStatsColumns(Optional.of("ab`")))
                .hasMessage("Invalid value for delta.dataSkippingStatsColumns property: ab`");
    }
}
