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
package io.trino.json;

import java.util.EnumSet;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/// Translates XQuery / SQL:2023 `like_regex` patterns for execution by Trino's regex engine
/// (Joni or RE2J).
///
/// **Known dialect gaps:** SQL:2023 `like_regex` defers to XQuery F&O 3.0 regex, but the pattern
/// is handed to Trino's `regexp_like`, which has different semantics for some escapes:
///
/// - `\s`, `\d`, `\w`: ASCII-only in both Joni and RE2J; Unicode in XQuery. Patterns relying on
///   Unicode category matching will return unexpected results.
/// - `\i` (XML name-start char), `\I` (complement), `\c` (XML name char), `\C` (complement):
///   XML name-class escapes don't exist in Java regex and will raise a pattern-syntax error.
///   Prefer explicit character classes.
/// - `(?x)`: XQuery defines flag `x` as "whitespace is ignored, `#` begins a comment." Joni
///   supports this identically to Java. RE2J does not support `(?x)` at all; patterns using the
///   `x` flag may fail to compile under RE2J.
/// - Flags accepted are XQuery 1.0's `s`, `m`, `i`, and `x` only. XQuery 3.0 added `q`
///   (interpret the pattern literally), which is rejected here even though some engines
///   (for example PostgreSQL) accept it; SQL:2023's normative reference is XQuery 1.0.
public final class XQueryRegex
{
    /// SQL:2023 / XQuery `like_regex` flag set. Each flag corresponds to a single-character
    /// identifier in the source `flag` string and maps to a Java inline-flag character.
    public enum Flag
    {
        CASE_INSENSITIVE('i'),
        MULTI_LINE('m'),
        DOT_ALL('s'),
        EXTENDED('x');

        private final char character;

        Flag(char character)
        {
            this.character = character;
        }

        public char character()
        {
            return character;
        }

        public static Flag fromCharacter(char character)
        {
            for (Flag flag : values()) {
                if (flag.character == character) {
                    return flag;
                }
            }
            throw new IllegalArgumentException("invalid XQuery regular expression flag: " + character);
        }
    }

    private XQueryRegex() {}

    /// Parses the SQL:2023 / XQuery flag string into the corresponding [Flag] set. Throws
    /// [IllegalArgumentException] on an unknown character or a duplicated flag.
    public static Set<Flag> parseFlags(String flags)
    {
        Set<Flag> result = EnumSet.noneOf(Flag.class);
        for (int i = 0; i < flags.length(); i++) {
            Flag flag = Flag.fromCharacter(flags.charAt(i));
            if (!result.add(flag)) {
                throw new IllegalArgumentException("duplicate XQuery regular expression flag: " + flag.character());
            }
        }
        return result;
    }

    public static String patternWithFlags(String pattern, Set<Flag> flags)
    {
        requireNonNull(pattern, "pattern is null");
        if (flags.isEmpty()) {
            return pattern;
        }
        StringBuilder inlineFlags = new StringBuilder();
        for (Flag flag : flags) {
            inlineFlags.append(flag.character());
        }
        return "(?" + inlineFlags + ")" + pattern;
    }

    /// Rejects regex syntax that is valid in Joni (Trino's `regexp_like` engine) but not in the
    /// SQL:2023 / XQuery F&O 3.0 regex grammar. This is a lightweight pre-validator — it scans
    /// the pattern for known Joni-isms rather than implementing the full XQuery grammar.
    ///
    /// TODO: replace with a full XQuery F&O 3.0 regex parser. The current implementation lets
    /// through any Joni construct that isn't on the explicit rejection list, so Joni-specific
    /// behavior outside that list can still leak through as accepted but non-spec syntax.
    public static void validatePattern(String pattern)
    {
        requireNonNull(pattern, "pattern is null");
        for (int i = 0; i < pattern.length(); ) {
            char c = pattern.charAt(i);
            if (c == '\\') {
                if (i + 1 >= pattern.length()) {
                    // bare trailing backslash; let Joni's compile produce the diagnostic
                    return;
                }
                char next = pattern.charAt(i + 1);
                if (next == 'x' && (i + 2 >= pattern.length() || pattern.charAt(i + 2) != '{')) {
                    throw new IllegalArgumentException("\\xHH numeric escape is not valid XQuery regex syntax; use \\x{HHHH}");
                }
                if (next == 'u') {
                    throw new IllegalArgumentException("\\" + "uHHHH numeric escape is not valid XQuery regex syntax; use \\x{HHHH}");
                }
                i += 2;
                continue;
            }
            if (c == '[' && i + 2 < pattern.length() && pattern.charAt(i + 1) == '[' && pattern.charAt(i + 2) == ':') {
                throw new IllegalArgumentException("POSIX bracket classes ([[:class:]]) are not valid XQuery regex syntax; use \\p{IsClass}");
            }
            if (c == '(' && i + 1 < pattern.length() && pattern.charAt(i + 1) == '?') {
                if (i + 2 >= pattern.length()) {
                    return;
                }
                char inner = pattern.charAt(i + 2);
                if (inner == '<') {
                    if (i + 3 < pattern.length() && (pattern.charAt(i + 3) == '=' || pattern.charAt(i + 3) == '!')) {
                        throw new IllegalArgumentException("lookbehind assertions ((?<=...), (?<!...)) are not valid XQuery regex syntax");
                    }
                    throw new IllegalArgumentException("named groups ((?<name>...)) are not valid XQuery regex syntax");
                }
                if (inner == '>') {
                    throw new IllegalArgumentException("atomic groups ((?>...)) are not valid XQuery regex syntax");
                }
                if (inner == '(') {
                    throw new IllegalArgumentException("conditional groups ((?(cond)...)) are not valid XQuery regex syntax");
                }
                if (inner == 'P') {
                    throw new IllegalArgumentException("Python-style named groups ((?P<name>...)) are not valid XQuery regex syntax");
                }
                if (inner == '=' || inner == '!') {
                    throw new IllegalArgumentException("lookahead assertions ((?=...), (?!...)) are not valid XQuery regex syntax");
                }
                if (inner == '-' || "imsx".indexOf(inner) >= 0) {
                    throw new IllegalArgumentException("inline regex flags ((?i), (?ims:...), (?-i)) are not valid XQuery regex syntax; pass flags as the like_regex flag argument");
                }
            }
            if ((c == '*' || c == '+' || c == '?' || c == '}') && i + 1 < pattern.length() && pattern.charAt(i + 1) == '+') {
                throw new IllegalArgumentException("possessive quantifiers (*+, ++, ?+, }+) are not valid XQuery regex syntax");
            }
            i++;
        }
    }
}
