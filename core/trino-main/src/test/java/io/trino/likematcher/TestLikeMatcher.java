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

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestLikeMatcher
{
    @Test
    public void test()
    {
        // min length short-circuit
        assertThat(match("__", "a")).isFalse();

        // max length short-circuit
        assertThat(match("__", "abcdefghi")).isFalse();

        // prefix short-circuit
        assertThat(match("a%", "xyz")).isFalse();

        // prefix match
        assertThat(match("a%", "a")).isTrue();
        assertThat(match("a%", "ab")).isTrue();
        assertThat(match("a_", "ab")).isTrue();

        // suffix short-circuit
        assertThat(match("%a", "xyz")).isFalse();

        // suffix match
        assertThat(match("%z", "z")).isTrue();
        assertThat(match("%z", "yz")).isTrue();
        assertThat(match("_z", "yz")).isTrue();

        // match literal
        assertThat(match("abcd", "abcd")).isTrue();

        // match one
        assertThat(match("_", "")).isFalse();
        assertThat(match("_", "a")).isTrue();
        assertThat(match("_", "ab")).isFalse();

        // match zero or more
        assertThat(match("%", "")).isTrue();
        assertThat(match("%", "a")).isTrue();
        assertThat(match("%", "ab")).isTrue();

        // non-strict matching
        assertThat(match("_%", "abcdefg")).isTrue();
        assertThat(match("_a%", "abcdefg")).isFalse();

        // strict matching
        assertThat(match("_ab_", "xabc")).isTrue();
        assertThat(match("_ab_", "xyxw")).isFalse();
        assertThat(match("_a%b_", "xaxxxbx")).isTrue();

        // optimization of consecutive _ and %
        assertThat(match("_%_%_%_%", "abcdefghij")).isTrue();

        assertThat(match("%a%a%a%a%a%a%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).isTrue();
        assertThat(match("%a%a%a%a%a%a%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab")).isTrue();
        assertThat(match("%a%b%a%b%a%b%", "aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")).isTrue();
        assertThat(match("%aaaa%bbbb%aaaa%bbbb%aaaa%bbbb%", "aaaabbbbaaaabbbbaaaabbbb")).isTrue();
        assertThat(match("%aaaaaaaaaaaaaaaaaaaaaaaaaa%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).isTrue();

        assertThat(match("%aab%bba%aab%bba%", "aaaabbbbaaaabbbbaaaa")).isTrue();
        assertThat(match("%aab%bba%aab%bba%", "aaaabbbbaaaabbbbcccc")).isFalse();
        assertThat(match("%abaca%", "abababababacabababa")).isTrue();
        assertThat(match("%bcccccccca%", "bbbbbbbbxax")).isFalse();
        assertThat(match("%bbxxxxxa%", "bbbxxxxaz")).isFalse();
        assertThat(match("%aaaaaaxaaaaaa%", "a".repeat(20) +
                "b".repeat(20) +
                "a".repeat(20) +
                "b".repeat(20) +
                "the quick brown fox jumps over the lazy dog")).isFalse();

        assertThat(match("%abaaa%", "ababaa")).isFalse();

        assertThat(match("%paya%", "papaya")).isTrue();
        assertThat(match("%paya%", "papapaya")).isTrue();
        assertThat(match("%paya%", "papapapaya")).isTrue();
        assertThat(match("%paya%", "papapapapaya")).isTrue();
        assertThat(match("%paya%", "papapapapapaya")).isTrue();

        // utf-8
        LikeMatcher singleOptimized = LikePattern.compile("_", Optional.empty(), true).getMatcher();
        LikeMatcher multipleOptimized = LikePattern.compile("_a%b_", Optional.empty(), true).getMatcher(); // prefix and suffix with _a and b_ to avoid optimizations
        LikeMatcher single = LikePattern.compile("_", Optional.empty(), false).getMatcher();
        LikeMatcher multiple = LikePattern.compile("_a%b_", Optional.empty(), false).getMatcher(); // prefix and suffix with _a and b_ to avoid optimizations
        for (int i = 0; i < Character.MAX_CODE_POINT; i++) {
            assertThat(singleOptimized.match(Character.toString(i).getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(single.match(Character.toString(i).getBytes(StandardCharsets.UTF_8))).isTrue();

            String value = "aa" + (char) i + "bb";
            assertThat(multipleOptimized.match(value.getBytes(StandardCharsets.UTF_8))).isTrue();
            assertThat(multiple.match(value.getBytes(StandardCharsets.UTF_8))).isTrue();
        }
    }

    @Test
    @Timeout(2)
    public void testExponentialBehavior()
    {
        assertThat(match("%a________________", "xyza1234567890123456")).isTrue();
    }

    @Test
    public void testEscape()
    {
        assertThat(match("-%", "%", '-')).isTrue();
        assertThat(match("-_", "_", '-')).isTrue();
        assertThat(match("--", "-", '-')).isTrue();

        assertThat(match("%$_%", "xxxxx_xxxxx", '$')).isTrue();
    }

    private static boolean match(String pattern, String value)
    {
        return match(pattern, value, Optional.empty());
    }

    private static boolean match(String pattern, String value, char escape)
    {
        return match(pattern, value, Optional.of(escape));
    }

    private static boolean match(String pattern, String value, Optional<Character> escape)
    {
        String padding = "++++";
        String padded = padding + value + padding;
        byte[] bytes = padded.getBytes(StandardCharsets.UTF_8);

        boolean optimizedWithoutPadding = LikeMatcher.compile(pattern, escape, true).match(value.getBytes(StandardCharsets.UTF_8));

        boolean optimizedWithPadding = LikeMatcher.compile(pattern, escape, true).match(bytes, padding.length(), bytes.length - padding.length() * 2);  // exclude padding
        assertThat(optimizedWithPadding).isEqualTo(optimizedWithoutPadding);

        boolean withoutPadding = LikeMatcher.compile(pattern, escape, false).match(value.getBytes(StandardCharsets.UTF_8));
        assertThat(withoutPadding).isEqualTo(optimizedWithoutPadding);

        boolean withPadding = LikeMatcher.compile(pattern, escape, false).match(bytes, padding.length(), bytes.length - padding.length() * 2);  // exclude padding
        assertThat(withPadding).isEqualTo(optimizedWithoutPadding);

        return withPadding;
    }
}
