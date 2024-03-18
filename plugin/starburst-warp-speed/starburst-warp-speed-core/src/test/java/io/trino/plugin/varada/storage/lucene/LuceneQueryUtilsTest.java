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
package io.trino.plugin.varada.storage.lucene;

import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.varada.storage.lucene.LuceneQueryUtils.likeToRegexp;
import static org.junit.jupiter.api.Assertions.assertEquals;

class LuceneQueryUtilsTest
{
    // This method is copy-pasted from our implementation in Trino
    @Test
    public void testLikeToRegexp()
    {
        assertEquals(likeToRegexp(Slices.utf8Slice("a_b_c")), "a.b.c");
        assertEquals(likeToRegexp(Slices.utf8Slice("a%b%c")), "a.*b.*c");
        assertEquals(likeToRegexp(Slices.utf8Slice("a%b_c")), "a.*b.c");
        assertEquals(likeToRegexp(Slices.utf8Slice("a[b")), "a\\[b");
        assertEquals(likeToRegexp(Slices.utf8Slice("a_\\b")), "a.\\\\b");
    }
}
