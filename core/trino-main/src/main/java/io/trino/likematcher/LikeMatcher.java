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

import io.airlift.slice.Slice;
import io.airlift.slice.SliceUtf8;
import io.airlift.slice.Slices;
import io.trino.likematcher.Pattern.Any;
import io.trino.likematcher.Pattern.Literal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.slice.SliceUtf8.lengthOfCodePoint;
import static io.airlift.slice.Slices.utf8Slice;

public class LikeMatcher
{
    private final int minSize;
    private final OptionalInt maxSize;
    private final Slice prefix;
    private final Slice suffix;
    private final Optional<Matcher> matcher;

    private LikeMatcher(
            int minSize,
            OptionalInt maxSize,
            Slice prefix,
            Slice suffix,
            Optional<Matcher> matcher)
    {
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.prefix = prefix;
        this.suffix = suffix;
        this.matcher = matcher;
    }

    public static LikeMatcher compile(Slice pattern)
    {
        return compile(pattern, Optional.empty(), true);
    }

    public static LikeMatcher compile(Slice pattern, Optional<Character> escape)
    {
        return compile(pattern, escape, true);
    }

    public static LikeMatcher compile(Slice pattern, Optional<Character> escape, boolean optimize)
    {
        List<Pattern> parsed = parse(pattern, escape);
        // Calculate minimum and maximum size for candidate strings
        // This is used for short-circuiting the match if the size of
        // the input is outside those bounds
        int minSize = 0;
        int maxSize = 0;
        boolean unbounded = false;
        for (Pattern expression : parsed) {
            switch (expression) {
                case Literal(Slice value) -> {
                    int length = value.length();
                    minSize += length;
                    maxSize += length;
                }
                case Pattern.ZeroOrMore _ -> unbounded = true;
                case Any any -> {
                    int length = any.length();
                    minSize += length;
                    maxSize += length * 4; // at most 4 bytes for a single UTF-8 codepoint
                }
            }
        }

        // Calculate exact match prefix and suffix
        // If the pattern starts and ends with a literal, we can perform a quick
        // exact match to short-circuit DFA evaluation
        Slice prefix = Slices.EMPTY_SLICE;
        Slice suffix = Slices.EMPTY_SLICE;

        int patternStart = 0;
        int patternEnd = parsed.size() - 1;
        if (!parsed.isEmpty() && parsed.getFirst() instanceof Literal(Slice value)) {
            prefix = value;
            patternStart++;
        }

        if (parsed.size() > 1 && parsed.getLast() instanceof Literal(Slice value)) {
            suffix = value;
            patternEnd--;
        }

        // If the pattern (after excluding constant prefix/suffixes) ends with an unbounded match (i.e., %)
        // we can perform a non-exact match and end as soon as the DFA reaches an accept state -- there
        // is no need to consume the remaining input
        // This section determines whether the pattern is a candidate for non-exact match.
        boolean exact = true; // whether to match to the end of the input
        if (patternStart <= patternEnd && parsed.get(patternEnd) instanceof Pattern.ZeroOrMore) {
            // guaranteed to be Any or ZeroOrMore because any Literal would've been turned into a suffix above
            exact = false;
            patternEnd--;
        }

        Optional<Matcher> matcher = Optional.empty();
        if (patternStart <= patternEnd) {
            boolean hasAny = false;
            boolean hasAnyAfterZeroOrMore = false;
            boolean foundZeroOrMore = false;
            for (int i = patternStart; i <= patternEnd; i++) {
                Pattern item = parsed.get(i);
                if (item instanceof Any) {
                    if (foundZeroOrMore) {
                        hasAnyAfterZeroOrMore = true;
                    }
                    hasAny = true;
                    break;
                }
                else if (item instanceof Pattern.ZeroOrMore) {
                    foundZeroOrMore = true;
                }
            }

            if (hasAny) {
                if (optimize && !hasAnyAfterZeroOrMore) {
                    matcher = Optional.of(new DenseDfaMatcher(parsed, patternStart, patternEnd, exact));
                }
                else {
                    matcher = Optional.of(new NfaMatcher(parsed, patternStart, patternEnd, exact));
                }
            }
            else {
                matcher = Optional.of(new FjsMatcher(parsed, patternStart, patternEnd, exact));
            }
        }

        return new LikeMatcher(
                minSize,
                unbounded ? OptionalInt.empty() : OptionalInt.of(maxSize),
                prefix,
                suffix,
                matcher);
    }

    public boolean match(Slice input)
    {
        return match(input, 0, input.length());
    }

    public boolean match(Slice input, int offset, int length)
    {
        if (length < minSize) {
            return false;
        }

        if (maxSize.isPresent() && length > maxSize.getAsInt()) {
            return false;
        }

        if (!startsWith(prefix, input, offset)) {
            return false;
        }

        if (!startsWith(suffix, input, offset + length - suffix.length())) {
            return false;
        }

        if (matcher.isPresent()) {
            return matcher.get().match(input, offset + prefix.length(), length - suffix.length() - prefix.length());
        }

        return true;
    }

    private boolean startsWith(Slice pattern, Slice input, int offset)
    {
        if (pattern.length() == 0) {
            return true;
        }
        return pattern.mismatch(0, pattern.length(), input, offset, pattern.length()) == -1;
    }

    static List<Pattern> parse(Slice pattern, Optional<Character> escape)
    {
        List<Pattern> result = new ArrayList<>();

        StringBuilder literal = new StringBuilder();
        int anyCount = 0;
        boolean anyUnbounded = false;
        boolean inEscape = false;

        int position = 0;
        while (position < pattern.length()) {
            int character = SliceUtf8.getCodePointAt(pattern, position);
            position += lengthOfCodePoint(character);
            if (inEscape) {
                if (character != '%' && character != '_' && character != escape.get()) {
                    throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
                }

                literal.appendCodePoint(character);
                inEscape = false;
            }
            else if (escape.isPresent() && character == escape.get()) {
                inEscape = true;

                if (anyCount != 0) {
                    result.add(new Any(anyCount));
                    anyCount = 0;
                }

                if (anyUnbounded) {
                    result.add(new Pattern.ZeroOrMore());
                    anyUnbounded = false;
                }
            }
            else if (character == '%' || character == '_') {
                if (!literal.isEmpty()) {
                    result.add(new Literal(utf8Slice(literal.toString())));
                    literal.setLength(0);
                }

                if (character == '%') {
                    anyUnbounded = true;
                }
                else {
                    anyCount++;
                }
            }
            else {
                if (anyCount != 0) {
                    result.add(new Any(anyCount));
                    anyCount = 0;
                }

                if (anyUnbounded) {
                    result.add(new Pattern.ZeroOrMore());
                    anyUnbounded = false;
                }

                literal.appendCodePoint(character);
            }
        }

        if (inEscape) {
            throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
        }

        if (!literal.isEmpty()) {
            result.add(new Literal(utf8Slice(literal.toString())));
        }
        else {
            if (anyCount != 0) {
                result.add(new Any(anyCount));
            }

            if (anyUnbounded) {
                result.add(new Pattern.ZeroOrMore());
            }
        }

        return result;
    }
}
