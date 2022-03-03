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

import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.Optional;

import static org.testng.Assert.assertEquals;

public class TestElasticsearchMetadata
{
    @Test
    public void testLikeToRegexp()
    {
        assertEquals(likeToRegexp("a_b_c", Optional.empty()), "a.b.c");
        assertEquals(likeToRegexp("a%b%c", Optional.empty()), "a.*b.*c");
        assertEquals(likeToRegexp("a%b_c", Optional.empty()), "a.*b.c");
        assertEquals(likeToRegexp("a[b", Optional.empty()), "a\\[b");
        assertEquals(likeToRegexp("a_\\_b", Optional.of("\\")), "a._b");
        assertEquals(likeToRegexp("a$_b", Optional.of("$")), "a_b");
        assertEquals(likeToRegexp("s_.m%ex\\t", Optional.of("$")), "s.\\.m.*ex\\\\t");
        assertEquals(likeToRegexp("\000%", Optional.empty()), "\000.*");
        assertEquals(likeToRegexp("\000%", Optional.of("\000")), "%");
    }

    private static String likeToRegexp(String pattern, Optional<String> escapeChar)
    {
        return ElasticsearchMetadata.likeToRegexp(Slices.utf8Slice(pattern), escapeChar.map(Slices::utf8Slice));
    }
}
