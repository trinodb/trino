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

import io.trino.likematcher.LikeMatcher;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.PageBuilderStatus;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.trino.type.LikePatternType.LIKE_PATTERN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestLikePatternType
{
    @Test
    public void testGetObject()
    {
        BlockBuilder blockBuilder = LIKE_PATTERN.createBlockBuilder(new PageBuilderStatus().createBlockBuilderStatus(), 10);
        LIKE_PATTERN.writeObject(blockBuilder, LikeMatcher.compile("helloX_world", Optional.of('X')));
        LIKE_PATTERN.writeObject(blockBuilder, LikeMatcher.compile("foo%_bar"));
        Block block = blockBuilder.build();

        LikeMatcher pattern = (LikeMatcher) LIKE_PATTERN.getObject(block, 0);
        assertThat(pattern.getPattern()).isEqualTo("helloX_world");
        assertThat(pattern.getEscape()).isEqualTo(Optional.of('X'));

        pattern = (LikeMatcher) LIKE_PATTERN.getObject(block, 1);
        assertThat(pattern.getPattern()).isEqualTo("foo%_bar");
        assertThat(pattern.getEscape()).isEqualTo(Optional.empty());
    }
}
