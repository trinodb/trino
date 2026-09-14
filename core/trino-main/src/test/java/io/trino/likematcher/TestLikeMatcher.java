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
package io.trino.likematcher;

import io.trino.type.LikePattern;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLikeMatcher
        extends AbstractTestLikeMatcher
{
    @Override
    protected LikePattern compile(String pattern, Optional<Character> escape)
    {
        return LikePattern.compile(pattern, escape);
    }

    @Test
    public void testWithoutOptimization()
    {
        assertMatches(TestLikeMatcher::compileWithoutOptimization);
    }

    @Test
    @Timeout(2)
    public void testExponentialBehaviorWithoutOptimization()
    {
        assertThat(match(TestLikeMatcher::compileWithoutOptimization, "%a________________", "xyza1234567890123456")).isTrue();
    }

    @Test
    public void testEscapeWithoutOptimization()
    {
        assertThat(match(TestLikeMatcher::compileWithoutOptimization, "-%", "%", '-')).isTrue();
        assertThat(match(TestLikeMatcher::compileWithoutOptimization, "-_", "_", '-')).isTrue();
        assertThat(match(TestLikeMatcher::compileWithoutOptimization, "--", "-", '-')).isTrue();

        assertThat(match(TestLikeMatcher::compileWithoutOptimization, "%$_%", "xxxxx_xxxxx", '$')).isTrue();
    }

    private static LikePattern compileWithoutOptimization(String pattern, Optional<Character> escape)
    {
        return LikePattern.compile(pattern, escape, false);
    }
}
