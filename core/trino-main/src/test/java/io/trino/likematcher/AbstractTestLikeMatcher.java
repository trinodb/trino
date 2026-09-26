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
import java.util.function.BiFunction;

import static io.airlift.slice.Slices.wrappedBuffer;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractTestLikeMatcher
{
    protected abstract LikePattern compile(String pattern, Optional<Character> escape);

    @Test
    public void test()
    {
        assertMatches(this::compile);
    }

    protected static void assertMatches(BiFunction<String, Optional<Character>, LikePattern> compile)
    {
        // min length short-circuit
        assertThat(match(compile, "__", "a")).isFalse();

        // max length short-circuit
        assertThat(match(compile, "__", "abcdefghi")).isFalse();

        // prefix short-circuit
        assertThat(match(compile, "a%", "xyz")).isFalse();

        // prefix match
        assertThat(match(compile, "a%", "a")).isTrue();
        assertThat(match(compile, "a%", "ab")).isTrue();
        assertThat(match(compile, "a_", "ab")).isTrue();

        // suffix short-circuit
        assertThat(match(compile, "%a", "xyz")).isFalse();

        // suffix match
        assertThat(match(compile, "%z", "z")).isTrue();
        assertThat(match(compile, "%z", "yz")).isTrue();
        assertThat(match(compile, "_z", "yz")).isTrue();

        // match literal
        assertThat(match(compile, "abcd", "abcd")).isTrue();

        // match one
        assertThat(match(compile, "_", "")).isFalse();
        assertThat(match(compile, "_", "a")).isTrue();
        assertThat(match(compile, "_", "ab")).isFalse();

        // match zero or more
        assertThat(match(compile, "%", "")).isTrue();
        assertThat(match(compile, "%", "a")).isTrue();
        assertThat(match(compile, "%", "ab")).isTrue();

        // non-strict matching
        assertThat(match(compile, "_%", "abcdefg")).isTrue();
        assertThat(match(compile, "_a%", "abcdefg")).isFalse();

        // strict matching
        assertThat(match(compile, "_ab_", "xabc")).isTrue();
        assertThat(match(compile, "_ab_", "xyxw")).isFalse();
        assertThat(match(compile, "_a%b_", "xaxxxbx")).isTrue();

        // optimization of consecutive _ and %
        assertThat(match(compile, "_%_%_%_%", "abcdefghij")).isTrue();

        assertThat(match(compile, "%a%a%a%a%a%a%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).isTrue();
        assertThat(match(compile, "%a%a%a%a%a%a%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab")).isTrue();
        assertThat(match(compile, "%a%b%a%b%a%b%", "aabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabbaabb")).isTrue();
        assertThat(match(compile, "%aaaa%bbbb%aaaa%bbbb%aaaa%bbbb%", "aaaabbbbaaaabbbbaaaabbbb")).isTrue();
        assertThat(match(compile, "%aaaaaaaaaaaaaaaaaaaaaaaaaa%", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).isTrue();

        assertThat(match(compile, "%aab%bba%aab%bba%", "aaaabbbbaaaabbbbaaaa")).isTrue();
        assertThat(match(compile, "%aab%bba%aab%bba%", "aaaabbbbaaaabbbbcccc")).isFalse();
        assertThat(match(compile, "%abaca%", "abababababacabababa")).isTrue();
        assertThat(match(compile, "%bcccccccca%", "bbbbbbbbxax")).isFalse();
        assertThat(match(compile, "%bbxxxxxa%", "bbbxxxxaz")).isFalse();
        assertThat(match(compile, "%aaaaaaxaaaaaa%", "a".repeat(20) +
                "b".repeat(20) +
                "a".repeat(20) +
                "b".repeat(20) +
                "the quick brown fox jumps over the lazy dog")).isFalse();

        assertThat(match(compile, "%abaaa%", "ababaa")).isFalse();

        assertThat(match(compile, "%paya%", "papaya")).isTrue();
        assertThat(match(compile, "%paya%", "papapaya")).isTrue();
        assertThat(match(compile, "%paya%", "papapapaya")).isTrue();
        assertThat(match(compile, "%paya%", "papapapapaya")).isTrue();
        assertThat(match(compile, "%paya%", "papapapapapaya")).isTrue();

        // utf-8
        LikePattern single = compile.apply("_", Optional.empty());
        LikePattern multiple = compile.apply("_a%b_", Optional.empty()); // prefix and suffix with _a and b_ to avoid optimizations
        for (int i = 0; i < Character.MAX_CODE_POINT; i++) {
            assertThat(single.matches(wrappedBuffer(Character.toString(i).getBytes(StandardCharsets.UTF_8)))).isTrue();

            String value = "aa" + (char) i + "bb";
            assertThat(multiple.matches(wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)))).isTrue();
        }
    }

    @Test
    @Timeout(2)
    public void testExponentialBehavior()
    {
        assertThat(match(this::compile, "%a________________", "xyza1234567890123456")).isTrue();
    }

    @Test
    public void testEscape()
    {
        assertThat(match(this::compile, "-%", "%", '-')).isTrue();
        assertThat(match(this::compile, "-_", "_", '-')).isTrue();
        assertThat(match(this::compile, "--", "-", '-')).isTrue();

        assertThat(match(this::compile, "%$_%", "xxxxx_xxxxx", '$')).isTrue();
    }

    protected static boolean match(BiFunction<String, Optional<Character>, LikePattern> compile, String pattern, String value)
    {
        return match(compile, pattern, value, Optional.empty());
    }

    protected static boolean match(BiFunction<String, Optional<Character>, LikePattern> compile, String pattern, String value, char escape)
    {
        return match(compile, pattern, value, Optional.of(escape));
    }

    private static boolean match(BiFunction<String, Optional<Character>, LikePattern> compile, String pattern, String value, Optional<Character> escape)
    {
        String padding = "++++";
        String padded = padding + value + padding;
        byte[] bytes = padded.getBytes(StandardCharsets.UTF_8);

        LikePattern matcher = compile.apply(pattern, escape);
        boolean withoutPadding = matcher.matches(wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)));

        boolean withPadding = matcher.matches(wrappedBuffer(bytes, padding.length(), bytes.length - padding.length() * 2));  // exclude padding
        assertThat(withPadding).isEqualTo(withoutPadding);

        return withPadding;
    }
}
