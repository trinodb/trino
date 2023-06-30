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

import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.trino.likematcher.LikeMatcher;
import io.trino.spi.TrinoException;
import io.trino.spi.function.LiteralParameter;
import io.trino.spi.function.LiteralParameters;
import io.trino.spi.function.ScalarFunction;
import io.trino.spi.function.SqlType;
import io.trino.spi.type.StandardTypes;

import java.util.Optional;

import static io.airlift.slice.SliceUtf8.getCodePointAt;
import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.Chars.padSpaces;
import static io.trino.util.Failures.checkCondition;

public final class LikeFunctions
{
    public static final String LIKE_FUNCTION_NAME = "$like";
    public static final String LIKE_PATTERN_FUNCTION_NAME = "$like_pattern";

    private LikeFunctions() {}

    @ScalarFunction(value = LIKE_FUNCTION_NAME, hidden = true)
    @LiteralParameters("x")
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeChar(@LiteralParameter("x") Long x, @SqlType("char(x)") Slice value, @SqlType(LikePatternType.NAME) LikeMatcher pattern)
    {
        return likeVarchar(padSpaces(value, x.intValue()), pattern);
    }

    // TODO: this should not be callable from SQL
    @ScalarFunction(value = LIKE_FUNCTION_NAME, hidden = true)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean likeVarchar(@SqlType("varchar") Slice value, @SqlType(LikePatternType.NAME) LikeMatcher matcher)
    {
        if (value.hasByteArray()) {
            return matcher.match(value.byteArray(), value.byteArrayOffset(), value.length());
        }
        return matcher.match(value.getBytes(), 0, value.length());
    }

    @ScalarFunction(value = LIKE_PATTERN_FUNCTION_NAME, hidden = true)
    @SqlType(LikePatternType.NAME)
    public static LikeMatcher likePattern(@SqlType("varchar") Slice pattern)
    {
        return LikeMatcher.compile(pattern.toStringUtf8(), Optional.empty(), false);
    }

    @ScalarFunction(value = LIKE_PATTERN_FUNCTION_NAME, hidden = true)
    @SqlType(LikePatternType.NAME)
    public static LikeMatcher likePattern(@SqlType("varchar") Slice pattern, @SqlType("varchar") Slice escape)
    {
        try {
            return LikeMatcher.compile(pattern.toStringUtf8(), getEscapeCharacter(Optional.of(escape)), false);
        }
        catch (RuntimeException e) {
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static boolean isMatchAllPattern(Slice pattern)
    {
        for (int i = 0; i < pattern.length(); i++) {
            int current = pattern.getByte(i);
            if (current != '%') {
                return false;
            }
        }
        return true;
    }

    public static boolean isLikePattern(Slice pattern, Optional<Slice> escape)
    {
        return patternConstantPrefixBytes(pattern, escape) < pattern.length();
    }

    public static int patternConstantPrefixBytes(Slice pattern, Optional<Slice> escape)
    {
        int escapeChar = getEscapeCharacter(escape)
                .map(c -> (int) c)
                .orElse(-1);

        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else if (escaped) {
                checkEscape(currentChar == '%' || currentChar == '_' || currentChar == escapeChar);
                escaped = false;
            }
            else if ((currentChar == '%') || (currentChar == '_')) {
                return position;
            }
            position += lengthOfCodePoint(currentChar);
        }
        checkEscape(!escaped);
        return position;
    }

    public static Slice unescapeLiteralLikePattern(Slice pattern, Optional<Slice> escape)
    {
        if (escape.isEmpty()) {
            return pattern;
        }

        int escapeChar = getEscapeCharacter(escape)
                .map(c -> (int) c)
                .orElse(-1);

        @SuppressWarnings("resource")
        DynamicSliceOutput output = new DynamicSliceOutput(pattern.length());
        boolean escaped = false;
        int position = 0;
        while (position < pattern.length()) {
            int currentChar = getCodePointAt(pattern, position);
            int lengthOfCodePoint = lengthOfCodePoint(currentChar);
            if (!escaped && (currentChar == escapeChar)) {
                escaped = true;
            }
            else {
                output.writeBytes(pattern, position, lengthOfCodePoint);
                escaped = false;
            }
            position += lengthOfCodePoint;
        }
        checkEscape(!escaped);
        return output.slice();
    }

    private static Optional<Character> getEscapeCharacter(Optional<Slice> escape)
    {
        if (escape.isEmpty()) {
            return Optional.empty();
        }

        String escapeString = escape.get().toStringUtf8();
        if (escapeString.length() != 1) {
            // non-BMP escape is not supported
            throw new TrinoException(INVALID_FUNCTION_ARGUMENT, "Escape string must be a single character");
        }

        return Optional.of(escapeString.charAt(0));
    }

    private static void checkEscape(boolean condition)
    {
        checkCondition(condition, INVALID_FUNCTION_ARGUMENT, "Escape character must be followed by '%%', '_' or the escape character itself");
    }
}
