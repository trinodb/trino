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

import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Syntax;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.type.JoniRegexp;

import java.util.Optional;

import static io.airlift.joni.constants.MetaChar.INEFFECTIVE_META_CHAR;
import static io.airlift.joni.constants.SyntaxProperties.OP_ASTERISK_ZERO_INF;
import static io.airlift.joni.constants.SyntaxProperties.OP_DOT_ANYCHAR;
import static io.airlift.joni.constants.SyntaxProperties.OP_LINE_ANCHOR;
import static io.trino.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.util.Failures.checkCondition;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class RegexLikeMatcher
        implements LikeMatcher
{
    private static final Syntax SYNTAX = new Syntax(
            OP_DOT_ANYCHAR | OP_ASTERISK_ZERO_INF | OP_LINE_ANCHOR,
            0,
            0,
            Option.NONE,
            new Syntax.MetaCharTable(
                    '\\',                           /* esc */
                    INEFFECTIVE_META_CHAR,          /* anychar '.' */
                    INEFFECTIVE_META_CHAR,          /* anytime '*' */
                    INEFFECTIVE_META_CHAR,          /* zero or one time '?' */
                    INEFFECTIVE_META_CHAR,          /* one or more time '+' */
                    INEFFECTIVE_META_CHAR));        /* anychar anytime */

    private final String pattern;
    private final Optional<Character> escape;
    private final JoniRegexp regexMatcher;

    public static RegexLikeMatcher compile(String pattern)
    {
        return compile(pattern, Optional.empty());
    }

    public static RegexLikeMatcher compile(String pattern, Optional<Character> escape)
    {
        StringBuilder regex = new StringBuilder(pattern.length() * 2);

        regex.append('^');
        boolean escaped = false;
        char escapeChar = escape.orElse((char) -1);
        for (char currentChar : pattern.toCharArray()) {
            checkEscape(!escaped || currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
            if (escape.isPresent() && !escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else {
                switch (currentChar) {
                    case '%' -> {
                        regex.append(escaped ? "%" : ".*");
                        escaped = false;
                    }
                    case '_' -> {
                        regex.append(escaped ? "_" : ".");
                        escaped = false;
                    }
                    default -> {
                        // escape special regex characters
                        switch (currentChar) {
                            case '\\', '^', '$', '.', '*' -> regex.append('\\');
                        }
                        regex.append(currentChar);
                        escaped = false;
                    }
                }
            }
        }
        checkEscape(!escaped);
        regex.append('$');

        byte[] bytes = regex.toString().getBytes(UTF_8);
        Regex joniRegex = new Regex(bytes, 0, bytes.length, Option.MULTILINE, NonStrictUTF8Encoding.INSTANCE, SYNTAX);
        return new RegexLikeMatcher(pattern, escape, new JoniRegexp(Slices.wrappedBuffer(bytes), joniRegex));
    }

    private RegexLikeMatcher(String pattern, Optional<Character> escape, JoniRegexp regexMatcher)
    {
        this.pattern = requireNonNull(pattern, "pattern is null");
        this.escape = requireNonNull(escape, "escape is null");
        this.regexMatcher = requireNonNull(regexMatcher, "regexMatcher is null");
    }

    @Override
    public String getPattern()
    {
        return pattern;
    }

    @Override
    public Optional<Character> getEscape()
    {
        return escape;
    }

    @Override
    public boolean match(byte[] input, int offset, int length)
    {
        Matcher matcher = regexMatcher.regex().matcher(input, offset, offset + length);
        return getMatchingOffset(matcher, offset, offset + length) != -1;
    }

    private static void checkEscape(boolean condition)
    {
        checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
    }

    private static int getMatchingOffset(Matcher matcher, int at, int range)
    {
        try {
            return matcher.matchInterruptible(at, range, Option.NONE);
        }
        catch (InterruptedException interruptedException) {
            Thread.currentThread().interrupt();
            throw new TrinoException(GENERIC_USER_ERROR, "" +
                    "Regular expression matching was interrupted, likely because it took too long. " +
                    "Regular expression in the worst case can have a catastrophic amount of backtracking and having exponential time complexity");
        }
    }
}
