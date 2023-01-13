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

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TestLikeMatcher
{
    private enum LikeMatcherType
    {
        DFA,
        REGEX
    }

    @Test(dataProvider = "likeMatcherTypes")
    public void test(LikeMatcherType type)
    {
        // min length short-circuit
        assertFalse(match(type, "__", "a"));

        // max length short-circuit
        assertFalse(match(type, "__", "abcdefghi"));

        // prefix short-circuit
        assertFalse(match(type, "a%", "xyz"));

        // prefix match
        assertTrue(match(type, "a%", "a"));
        assertTrue(match(type, "a%", "ab"));
        assertTrue(match(type, "a_", "ab"));

        // suffix short-circuit
        assertFalse(match(type, "%a", "xyz"));

        // suffix match
        assertTrue(match(type, "%z", "z"));
        assertTrue(match(type, "%z", "yz"));
        assertTrue(match(type, "_z", "yz"));

        // match literal
        assertTrue(match(type, "abcd", "abcd"));

        // match one
        assertFalse(match(type, "_", ""));
        assertTrue(match(type, "_", "a"));
        assertFalse(match(type, "_", "ab"));

        // match zero or more
        assertTrue(match(type, "%", ""));
        assertTrue(match(type, "%", "a"));
        assertTrue(match(type, "%", "ab"));

        // non-strict matching
        assertTrue(match(type, "_%", "abcdefg"));
        assertFalse(match(type, "_a%", "abcdefg"));

        // strict matching
        assertTrue(match(type, "_ab_", "xabc"));
        assertFalse(match(type, "_ab_", "xyxw"));
        assertTrue(match(type, "_a%b_", "xaxxxbx"));

        // optimization of consecutive _ and %
        assertTrue(match(type, "_%_%_%_%", "abcdefghij"));

        // utf-8
        LikeMatcher single = compile(type, "_", Optional.empty());
        LikeMatcher multiple = compile(type, "_a%b_", Optional.empty()); // prefix and suffix with _a and b_ to avoid optimizations
        for (int i = 0; i < Character.MAX_CODE_POINT; i++) {
            assertTrue(single.match(Character.toString(i).getBytes(StandardCharsets.UTF_8)));

            String value = "aa" + (char) i + "bb";
            assertTrue(multiple.match(value.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test(dataProvider = "likeMatcherTypes")
    public void testEscape(LikeMatcherType type)
    {
        assertTrue(match(type, "-%", "%", '-'));
        assertTrue(match(type, "-_", "_", '-'));
        assertTrue(match(type, "--", "-", '-'));
    }

    @DataProvider
    public Object[][] likeMatcherTypes()
    {
        return new Object[][] {{LikeMatcherType.DFA}, {LikeMatcherType.REGEX}};
    }

    private static boolean match(LikeMatcherType type, String pattern, String value)
    {
        return match(type, pattern, value, Optional.empty());
    }

    private static boolean match(LikeMatcherType type, String pattern, String value, char escape)
    {
        return match(type, pattern, value, Optional.of(escape));
    }

    private static boolean match(LikeMatcherType type, String pattern, String value, Optional<Character> escape)
    {
        String padding = "++++";
        String padded = padding + value + padding;
        byte[] bytes = padded.getBytes(StandardCharsets.UTF_8);

        boolean withoutPadding = compile(type, pattern, escape).match(value.getBytes(StandardCharsets.UTF_8));
        boolean withPadding = compile(type, pattern, escape).match(bytes, padding.length(), bytes.length - padding.length() * 2);  // exclude padding

        assertEquals(withoutPadding, withPadding);
        return withPadding;
    }

    private static LikeMatcher compile(LikeMatcherType type, String pattern, Optional<Character> escape)
    {
        if (type == LikeMatcherType.DFA) {
            return DfaLikeMatcher.compile(pattern, escape);
        }
        else {
            return RegexLikeMatcher.compile(pattern, escape);
        }
    }
}
