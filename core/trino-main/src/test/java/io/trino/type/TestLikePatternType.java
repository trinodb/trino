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
package io.trino.type;

import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.type.LikeLibrary.REGULATOR;
import static io.trino.type.LikeLibrary.TRINO;
import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLikePatternType
{
    @ParameterizedTest
    @EnumSource(LikeLibrary.class)
    public void testGetObject(LikeLibrary library)
    {
        BlockBuilder blockBuilder = LIKE_PATTERN.createBlockBuilder(null, 10);
        LIKE_PATTERN.writeObject(blockBuilder, LikePattern.compile("helloX_world", Optional.of('X'), library));
        LIKE_PATTERN.writeObject(blockBuilder, LikePattern.compile("foo%_bar", Optional.empty(), library));
        Block block = blockBuilder.build();

        LikePattern pattern = (LikePattern) LIKE_PATTERN.getObject(block, 0);
        assertThat(pattern.getPattern()).isEqualTo("helloX_world");
        assertThat(pattern.getEscape()).isEqualTo(Optional.of('X'));
        assertThat(pattern.getLibrary()).isEqualTo(library);
        assertThat(pattern.matches(utf8Slice("hello_world"))).isTrue();
        assertThat(pattern.matches(utf8Slice("helloX_world"))).isFalse();

        pattern = (LikePattern) LIKE_PATTERN.getObject(block, 1);
        assertThat(pattern.getPattern()).isEqualTo("foo%_bar");
        assertThat(pattern.getEscape()).isEqualTo(Optional.empty());
        assertThat(pattern.getLibrary()).isEqualTo(library);
        assertThat(pattern.matches(utf8Slice("fooXYbar"))).isTrue();
        assertThat(pattern.matches(utf8Slice("foobar"))).isFalse();
    }

    @Test
    public void testLibraryInCacheKey()
    {
        LikePattern trino = LikePattern.compile("a%", Optional.empty(), TRINO);
        LikePattern regulator = LikePattern.compile("a%", Optional.empty(), REGULATOR);
        assertThat(trino).isEqualTo(LikePattern.compile("a%", Optional.empty(), TRINO));
        assertThat(regulator).isEqualTo(LikePattern.compile("a%", Optional.empty(), REGULATOR));
        assertThat(trino).isNotEqualTo(regulator);
    }
}
