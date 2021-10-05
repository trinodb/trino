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

import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

public class TestElasticsearchMetadata
{
    @Test
    public void testLikeToRegexp()
    {
        assertEquals(ElasticsearchMetadata.likeToRegexp("a_b_c"), "a.b.c");
        assertEquals(ElasticsearchMetadata.likeToRegexp("a%b%c"), "a.*b.*c");
        assertEquals(ElasticsearchMetadata.likeToRegexp("a%b_c"), "a.*b.c");
        assertEquals(ElasticsearchMetadata.likeToRegexp("a[b"), "a\\[b");
        assertEquals(ElasticsearchMetadata.likeToRegexp("a_\\b"), "a.\\\\b");
    }
}
