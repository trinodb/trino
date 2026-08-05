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
package io.trino.plugin.elasticsearch;

import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.elasticsearch.ElasticsearchMetadata.likePrefix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLikePrefix
{
    @Test
    public void testTrailingWildcardIsPrefix()
    {
        // 'abc%' matches everything starting with abc -> Elasticsearch prefix query on "abc"
        assertThat(likePrefix(utf8Slice("abc%"), Optional.empty())).contains("abc");
    }

    @Test
    public void testMatchAllIsEmptyPrefix()
    {
        // '%' matches every value; an empty prefix is still a valid (match-all) prefix query
        assertThat(likePrefix(utf8Slice("%"), Optional.empty())).contains("");
    }

    @Test
    public void testWildcardInMiddleIsNotPrefix()
    {
        assertThat(likePrefix(utf8Slice("a%b"), Optional.empty())).isEmpty();
    }

    @Test
    public void testSingleCharWildcardIsNotPrefix()
    {
        assertThat(likePrefix(utf8Slice("a_c"), Optional.empty())).isEmpty();
        assertThat(likePrefix(utf8Slice("ab_%"), Optional.empty())).isEmpty();
    }

    @Test
    public void testNoWildcardIsNotPrefix()
    {
        // An exact match is left to regexp (or handled as a domain), not a prefix query
        assertThat(likePrefix(utf8Slice("abc"), Optional.empty())).isEmpty();
    }

    @Test
    public void testEscapedWildcardBecomesLiteralPrefix()
    {
        // 'a\%b%' ESCAPE '\' -> literal prefix "a%b" followed by a trailing wildcard
        assertThat(likePrefix(utf8Slice("a\\%b%"), Optional.of(utf8Slice("\\")))).contains("a%b");
    }

    @Test
    public void testEscapedTrailingWildcardIsNotPrefix()
    {
        // 'abc\%' ESCAPE '\' -> literal "abc%" with no trailing wildcard, so it is an exact match
        assertThat(likePrefix(utf8Slice("abc\\%"), Optional.of(utf8Slice("\\")))).isEmpty();
    }
}
