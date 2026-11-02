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
package io.trino.spi.cache;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TestCacheKey
{
    @Test
    void testToStringIsInjective()
    {
        assertThat(CacheKey.of("a|b", "c").toString())
                .isNotEqualTo(CacheKey.of("a", "b|c").toString());
        assertThat(CacheKey.of("a\\", "|b").toString())
                .isNotEqualTo(CacheKey.of("a", "\\|b").toString());
        assertThat(CacheKey.of("a", "b").toString()).isEqualTo("a|b");
        assertThat(CacheKey.of("a|b", "c").toString()).isEqualTo("a\\|b|c");
    }

    @Test
    void testAppend()
    {
        assertThat(CacheKey.of("a").append("b")).isEqualTo(CacheKey.of("a", "b"));
        assertThat(CacheKey.of("a", "b").append(CacheKey.of("c", "d"))).isEqualTo(CacheKey.of("a", "b", "c", "d"));
    }

    @Test
    void testStartsWith()
    {
        assertThat(CacheKey.of("a", "b", "c").startsWith(CacheKey.of("a", "b"))).isTrue();
        assertThat(CacheKey.of("a", "b").startsWith(CacheKey.of("a", "b"))).isTrue();
        assertThat(CacheKey.of("a", "b").startsWith(CacheKey.of("a", "b", "c"))).isFalse();
        assertThat(CacheKey.of("ab", "c").startsWith(CacheKey.of("a"))).isFalse();
    }
}
