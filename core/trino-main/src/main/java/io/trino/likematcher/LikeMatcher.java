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

import io.trino.likematcher.Pattern.Any;
import io.trino.likematcher.Pattern.Literal;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LikeMatcher
{
    private final String pattern;
    private final Optional<Character> escape;

    private final int minSize;
    private final OptionalInt maxSize;
    private final byte[] prefix;
    private final byte[] suffix;
    private final Optional<DenseDfaMatcher> matcher;

    private LikeMatcher(
            String pattern,
            Optional<Character> escape,
            int minSize,
            OptionalInt maxSize,
            byte[] prefix,
            byte[] suffix,
            Optional<DenseDfaMatcher> matcher)
    {
        this.pattern = pattern;
        this.escape = escape;
        this.minSize = minSize;
        this.maxSize = maxSize;
        this.prefix = prefix;
        this.suffix = suffix;
        this.matcher = matcher;
    }

    public String getPattern()
    {
        return pattern;
    }

    public Optional<Character> getEscape()
    {
        return escape;
    }

    public static LikeMatcher compile(String pattern)
    {
        return compile(pattern, Optional.empty());
    }

    public static LikeMatcher compile(String pattern, Optional<Character> escape)
    {
        List<Pattern> parsed = parse(pattern, escape);
        List<Pattern> optimized = optimize(parsed);

        // Calculate minimum and maximum size for candidate strings
        // This is used for short-circuiting the match if the size of
        // the input is outside those bounds
        int minSize = 0;
        int maxSize = 0;
        boolean unbounded = false;
        for (Pattern expression : optimized) {
            if (expression instanceof Literal literal) {
                int length = literal.value().getBytes(UTF_8).length;
                minSize += length;
                maxSize += length;
            }
            else if (expression instanceof Any any) {
                int length = any.min();
                minSize += length;
                maxSize += length * 4; // at most 4 bytes for a single UTF-8 codepoint

                unbounded = unbounded || any.unbounded();
            }
            else {
                throw new UnsupportedOperationException("Not supported: " + expression.getClass().getName());
            }
        }

        // Calculate exact match prefix and suffix
        // If the pattern starts and ends with a literal, we can perform a quick
        // exact match to short-circuit DFA evaluation
        byte[] prefix = new byte[0];
        byte[] suffix = new byte[0];
        List<Pattern> middle = new ArrayList<>();
        for (int i = 0; i < optimized.size(); i++) {
            Pattern expression = optimized.get(i);

            if (i == 0) {
                if (expression instanceof Literal literal) {
                    prefix = literal.value().getBytes(UTF_8);
                    continue;
                }
            }
            else if (i == optimized.size() - 1) {
                if (expression instanceof Literal literal) {
                    suffix = literal.value().getBytes(UTF_8);
                    continue;
                }
            }

            middle.add(expression);
        }

        // If the pattern (after excluding constant prefix/suffixes) ends with an unbounded match (i.e., %)
        // we can perform a non-exact match and end as soon as the DFA reaches an accept state -- there
        // is no need to consume the remaining input
        // This section determines whether the pattern is a candidate for non-exact match.
        boolean exact = true; // whether to match to the end of the input
        if (!middle.isEmpty()) {
            // guaranteed to be Any because any Literal would've been turned into a suffix above
            Any last = (Any) middle.get(middle.size() - 1);
            if (last.unbounded()) {
                exact = false;

                // Since the matcher will stop early, no need for an unbounded matcher (it produces a simpler DFA)
                if (last.min() == 0) {
                    // We'd end up with an empty string match at the end, so just remove it
                    middle.remove(middle.size() - 1);
                }
                else {
                    middle.set(middle.size() - 1, new Any(last.min(), false));
                }
            }
        }

        Optional<DenseDfaMatcher> matcher = Optional.empty();
        if (!middle.isEmpty()) {
            matcher = Optional.of(DenseDfaMatcher.newInstance(makeNfa(middle).toDfa(), exact));
        }

        return new LikeMatcher(
                pattern,
                escape,
                minSize,
                unbounded ? OptionalInt.empty() : OptionalInt.of(maxSize),
                prefix,
                suffix,
                matcher);
    }

    public boolean match(byte[] input)
    {
        return match(input, 0, input.length);
    }

    public boolean match(byte[] input, int offset, int length)
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

        if (!startsWith(suffix, input, offset + length - suffix.length)) {
            return false;
        }

        if (matcher.isPresent()) {
            return matcher.get().match(input, offset + prefix.length, length - suffix.length - prefix.length);
        }

        return true;
    }

    private boolean startsWith(byte[] pattern, byte[] input, int offset)
    {
        for (int i = 0; i < pattern.length; i++) {
            if (pattern[i] != input[offset + i]) {
                return false;
            }
        }

        return true;
    }

    private static List<Pattern> parse(String pattern, Optional<Character> escape)
    {
        List<Pattern> result = new ArrayList<>();

        StringBuilder literal = new StringBuilder();
        boolean inEscape = false;
        for (int i = 0; i < pattern.length(); i++) {
            char character = pattern.charAt(i);

            if (inEscape) {
                if (character != '%' && character != '_' && character != escape.get()) {
                    throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
                }
                literal.append(character);
                inEscape = false;
            }
            else if (escape.isPresent() && character == escape.get()) {
                inEscape = true;
            }
            else if (character == '%' || character == '_') {
                if (literal.length() != 0) {
                    result.add(new Literal(literal.toString()));
                    literal = new StringBuilder();
                }

                if (character == '%') {
                    result.add(new Any(0, true));
                }
                else {
                    result.add(new Any(1, false));
                }
            }
            else {
                literal.append(character);
            }
        }

        if (inEscape) {
            throw new IllegalArgumentException("Escape character must be followed by '%', '_' or the escape character itself");
        }

        if (literal.length() != 0) {
            result.add(new Literal(literal.toString()));
        }

        return result;
    }

    private static List<Pattern> optimize(List<Pattern> pattern)
    {
        if (pattern.isEmpty()) {
            return pattern;
        }

        List<Pattern> result = new ArrayList<>();

        int anyPatternStart = -1;
        for (int i = 0; i < pattern.size(); i++) {
            Pattern current = pattern.get(i);

            if (anyPatternStart == -1 && current instanceof Any) {
                anyPatternStart = i;
            }
            else if (current instanceof Literal) {
                if (anyPatternStart != -1) {
                    result.add(collapse(pattern, anyPatternStart, i));
                }

                result.add(current);
                anyPatternStart = -1;
            }
        }

        if (anyPatternStart != -1) {
            result.add(collapse(pattern, anyPatternStart, pattern.size()));
        }

        return result;
    }

    /**
     * Collapses a sequence of consecutive Any items
     */
    private static Any collapse(List<Pattern> pattern, int start, int end)
    {
        int min = 0;
        boolean unbounded = false;

        for (int i = start; i < end; i++) {
            Any any = (Any) pattern.get(i);

            min += any.min();
            unbounded = unbounded || any.unbounded();
        }

        return new Any(min, unbounded);
    }

    private static NFA makeNfa(List<Pattern> pattern)
    {
        checkArgument(!pattern.isEmpty(), "pattern is empty");

        NFA.Builder builder = new NFA.Builder();

        NFA.State state = builder.addStartState();

        for (Pattern item : pattern) {
            if (item instanceof Literal literal) {
                for (byte current : literal.value().getBytes(UTF_8)) {
                    state = matchByte(builder, state, current);
                }
            }
            else if (item instanceof Any any) {
                NFA.State previous;
                int i = 0;
                do {
                    previous = state;
                    state = matchSingleUtf8(builder, state);
                    i++;
                }
                while (i < any.min());

                if (any.min() == 0) {
                    builder.addTransition(previous, new NFA.Epsilon(), state);
                }

                if (any.unbounded()) {
                    builder.addTransition(state, new NFA.Epsilon(), previous);
                }
            }
            else {
                throw new UnsupportedOperationException("Not supported: " + item.getClass().getName());
            }
        }

        builder.setAccept(state);

        return builder.build();
    }

    private static NFA.State matchByte(NFA.Builder builder, NFA.State state, byte value)
    {
        NFA.State next = builder.addState();
        builder.addTransition(state, new NFA.Value(value), next);
        return next;
    }

    private static NFA.State matchSingleUtf8(NFA.Builder builder, NFA.State start)
    {
        /*
            Implements a state machine to recognize UTF-8 characters.

                  11110xxx       10xxxxxx       10xxxxxx       10xxxxxx
              O ───────────► O ───────────► O ───────────► O ───────────► O
              │                             ▲              ▲              ▲
              ├─────────────────────────────┘              │              │
              │          1110xxxx                          │              │
              │                                            │              │
              ├────────────────────────────────────────────┘              │
              │                   110xxxxx                                │
              │                                                           │
              └───────────────────────────────────────────────────────────┘
                                        0xxxxxxx
        */

        NFA.State next = builder.addState();

        builder.addTransition(start, new NFA.Prefix(0, 1), next);

        NFA.State state1 = builder.addState();
        NFA.State state2 = builder.addState();
        NFA.State state3 = builder.addState();

        builder.addTransition(start, new NFA.Prefix(0b11110, 5), state1);
        builder.addTransition(start, new NFA.Prefix(0b1110, 4), state2);
        builder.addTransition(start, new NFA.Prefix(0b110, 3), state3);

        builder.addTransition(state1, new NFA.Prefix(0b10, 2), state2);
        builder.addTransition(state2, new NFA.Prefix(0b10, 2), state3);
        builder.addTransition(state3, new NFA.Prefix(0b10, 2), next);

        return next;
    }
}
