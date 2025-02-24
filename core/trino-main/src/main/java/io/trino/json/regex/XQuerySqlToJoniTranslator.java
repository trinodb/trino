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
package io.trino.json.regex;

import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Syntax;
import io.trino.json.regex.Quantifier.AtLeast;
import io.trino.json.regex.Quantifier.Exact;
import io.trino.json.regex.Quantifier.ExplicitRange;
import io.trino.json.regex.Quantifier.One;
import io.trino.json.regex.Quantifier.OneOrMore;
import io.trino.json.regex.Quantifier.ZeroOrMore;
import io.trino.json.regex.Quantifier.ZeroOrOne;

import java.util.Iterator;
import java.util.List;

import static io.airlift.joni.constants.MetaChar.INEFFECTIVE_META_CHAR;
import static io.airlift.joni.constants.SyntaxProperties.BACKSLASH_ESCAPE_IN_CC;
import static io.airlift.joni.constants.SyntaxProperties.DIFFERENT_LEN_ALT_LOOK_BEHIND;
import static io.airlift.joni.constants.SyntaxProperties.OP2_CCLASS_SET_OP;
import static io.airlift.joni.constants.SyntaxProperties.OP2_ESC_P_BRACE_CHAR_PROPERTY;
import static io.airlift.joni.constants.SyntaxProperties.OP2_QMARK_GROUP_EFFECT;
import static io.airlift.joni.constants.SyntaxProperties.OP_ASTERISK_ZERO_INF;
import static io.airlift.joni.constants.SyntaxProperties.OP_BRACE_INTERVAL;
import static io.airlift.joni.constants.SyntaxProperties.OP_BRACKET_CC;
import static io.airlift.joni.constants.SyntaxProperties.OP_DECIMAL_BACKREF;
import static io.airlift.joni.constants.SyntaxProperties.OP_DOT_ANYCHAR;
import static io.airlift.joni.constants.SyntaxProperties.OP_ESC_AZ_BUF_ANCHOR;
import static io.airlift.joni.constants.SyntaxProperties.OP_ESC_CONTROL_CHARS;
import static io.airlift.joni.constants.SyntaxProperties.OP_ESC_X_BRACE_HEX8;
import static io.airlift.joni.constants.SyntaxProperties.OP_LPAREN_SUBEXP;
import static io.airlift.joni.constants.SyntaxProperties.OP_PLUS_ONE_INF;
import static io.airlift.joni.constants.SyntaxProperties.OP_QMARK_ZERO_ONE;
import static io.airlift.joni.constants.SyntaxProperties.OP_VBAR_ALT;
import static io.trino.json.regex.MultiCharacterEscape.WHITESPACE_CHARACTER_SEQUENCES;
import static io.trino.json.regex.XQuerySqlEncoding.isPublicCharacterProperty;
import static java.nio.charset.StandardCharsets.UTF_8;

final class XQuerySqlToJoniTranslator
{
    private static final Syntax SYNTAX = new Syntax(
            OP_VBAR_ALT
                    | OP_DOT_ANYCHAR
                    | OP_QMARK_ZERO_ONE
                    | OP_ASTERISK_ZERO_INF
                    | OP_PLUS_ONE_INF
                    | OP_BRACE_INTERVAL
                    | OP_ESC_CONTROL_CHARS
                    | OP_ESC_X_BRACE_HEX8
                    | OP_BRACKET_CC
                    | OP_LPAREN_SUBEXP
                    | OP_DECIMAL_BACKREF
                    | OP_ESC_AZ_BUF_ANCHOR,
            OP2_ESC_P_BRACE_CHAR_PROPERTY | OP2_QMARK_GROUP_EFFECT | OP2_CCLASS_SET_OP,
            BACKSLASH_ESCAPE_IN_CC | DIFFERENT_LEN_ALT_LOOK_BEHIND,
            Option.NONE,
            // Only the escape metacharacter ('\') needs to be defined since Joni does not have a predefined default for it.
            // The rest of the metacharacters have defaults that either match the XQuery syntax or are not used.
            new Syntax.MetaCharTable(
                    '\\',
                    INEFFECTIVE_META_CHAR,
                    INEFFECTIVE_META_CHAR,
                    INEFFECTIVE_META_CHAR,
                    INEFFECTIVE_META_CHAR,
                    INEFFECTIVE_META_CHAR));

    private XQuerySqlToJoniTranslator() {}

    public static Regex translate(RegularExpression regularExpression, boolean dotAllMode, boolean multiLineMode, String pattern)
    {
        RegularExpressionTranslatingVisitor visitor = new RegularExpressionTranslatingVisitor(dotAllMode, multiLineMode, pattern);
        visitor.process(regularExpression);
        String joniPattern = visitor.getTranslatedPattern();
        byte[] joniPatternBytes = joniPattern.getBytes(UTF_8);

        int options = Option.SINGLELINE;
        if (dotAllMode) {
            options |= Option.MULTILINE; // In Joni, MULTILINE means that '.' matches any character, including '\n'.
        }

        return new Regex(joniPatternBytes, 0, joniPatternBytes.length, options, XQuerySqlEncoding.INSTANCE, SYNTAX);
    }

    private static class RegularExpressionTranslatingVisitor
            extends RegexTreeVisitor<Void>
    {
        private final StringBuilder translated;
        private final boolean dotAllMode;
        private final boolean multiLineMode;

        public RegularExpressionTranslatingVisitor(boolean dotAllMode, boolean multiLineMode, String pattern)
        {
            this.dotAllMode = dotAllMode;
            this.multiLineMode = multiLineMode;
            this.translated = new StringBuilder(pattern.length());
        }

        public String getTranslatedPattern()
        {
            return translated.toString();
        }

        @Override
        protected Void visitRegexNode(RegexNode node)
        {
            throw new UnsupportedOperationException("%s.visit%s is not implemented".formatted(getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected Void visitRegularExpression(RegularExpression node)
        {
            Iterator<Branch> iterator = node.branches().iterator();
            while (iterator.hasNext()) {
                Branch branch = iterator.next();
                process(branch);
                if (iterator.hasNext()) {
                    translated.append('|');
                }
            }
            return null;
        }

        @Override
        protected Void visitBranch(Branch node)
        {
            for (Piece piece : node.pieces()) {
                process(piece);
            }
            return null;
        }

        @Override
        protected Void visitPiece(Piece node)
        {
            process(node.atom());
            switch (node.quantifier()) {
                case ZeroOrOne _ -> translated.append('?');
                case ZeroOrMore _ -> translated.append('*');
                case OneOrMore _ -> translated.append('+');
                case Exact exact -> {
                    translated.append('{');
                    translated.append(exact.count());
                    translated.append('}');
                }
                case AtLeast atLeast -> {
                    translated.append('{');
                    translated.append(atLeast.count());
                    translated.append(",}");
                }
                case ExplicitRange explicitRange -> {
                    translated.append('{');
                    translated.append(explicitRange.min());
                    translated.append(',');
                    translated.append(explicitRange.max());
                    translated.append('}');
                }
                case One _ -> {}
            }
            return null;
        }

        @Override
        protected Void visitCharacterClassExpression(CharacterClassExpression node)
        {
            // The ISO/IEC 9075-2:2016(E) standard specifies that the multi-character escape "\s" matches
            // (among other characters) any two-character sequence identified as a newline sequence by
            // Unicode Technical Standard #18 (https://www.unicode.org/reports/tr18/#Line_Boundaries).
            // It turns out there is only one such two-character sequence: CRLF.
            //
            // Joni does not support defining character classes that include multi-character sequences.
            // To work around this limitation, the code below extracts the CRLF sequence from the source
            // character class expression and builds an alternation that matches either CRLF or any other
            // character from the source expression.
            CharacterClassExpressionTranslatingVisitor visitor = new CharacterClassExpressionTranslatingVisitor();
            boolean includesCrlf = visitor.process(node);
            String translatedExpression = visitor.getTranslatedPattern();
            if (includesCrlf) {
                // The following non-capturing group consists of two alternatives:
                // 1. The first matches the \x{D}\x{A} (CRLF) sequence.
                // 2. The second matches any single character sequence from the source character class
                //    expression that is not preceded by \x{D}\x{A}. This prevents incorrectly matching only
                //    \x{A} (LF) or \x{D} (CR) in the sequence \x{D}\x{A} during backtracking. According to
                //    Unicode Technical Standard #18, \x{D}\x{A} takes precedence over \x{D} and \x{A}.
                //    For example, \s\s should not match \r\n since the first \s covers the entire string.
                //    Without the negative lookahead, both \s and \s\s would match \r\n.
                translated.append("(?:\\x{D}\\x{A}|(?!\\x{D}\\x{A})");
                translated.append(translatedExpression);
                translated.append(")");
            }
            else {
                translated.append(translatedExpression);
            }
            return null;
        }

        @Override
        protected Void visitNormalCharacter(NormalCharacter node)
        {
            translated.appendCodePoint(node.codePoint());
            return null;
        }

        @Override
        protected Void visitWildcardEscape(WildcardEscape node)
        {
            // The ISO/IEC 9075-2:2016(E) standard (section 9.22) modifies the semantics of the wildcard escape
            // metacharacter ('.') compared to the XSD 1.0 specification (https://www.w3.org/TR/xquery-operators/#regex-syntax).
            // The ISO/IEC 9075-2:2016(E) standard derives the semantics of '.' from Unicode Technical Standard #18
            // (https://www.unicode.org/reports/tr18/#Line_Boundaries).
            //
            // The difference lies in the set of characters matched by '.'. The XSD specification prescribes that,
            // by default, '.' matches all characters except \x{A} (LF), while in "dot-all" mode it matches any character,
            // including LF. According to Unicode Technical Standard #18, in dot-all mode, the following newline
            // sequences are included: \x{A}, \x{B}, \x{C}, \x{D}, \x{85}, \x{2028}, \x{2029}, and \x{D}\x{A} (CRLF).
            // Additionally, it is recommended that CRLF be matched as a single character.
            if (dotAllMode) {
                // Special handling of CRLF to match it as a single character.
                translated.append("(?:\\x{D}\\x{A}|(?!\\x{D}\\x{A}).)");
            }
            else {
                // Special handling of CRLF in default mode is not required. The following regex is sufficient to
                // reject CR (\x{D}), LF (\x{A}), and CRLF (\x{D}\x{A}).
                translated.append("[^\\x{A}-\\x{D}\\x{85}\\x{2028}\\x{2029}]");
            }
            return null;
        }

        @Override
        protected Void visitEndMetaCharacter(EndMetaCharacter node)
        {
            // The ISO/IEC 9075-2:2016(E) standard (section 9.22) modifies the semantics of the end-of-line/string
            // metacharacter ('$') compared to the XSD 1.0 specification (https://www.w3.org/TR/xquery-operators/#regex-syntax).
            // The ISO/IEC 9075-2:2016(E) standard derives the semantics of '$' from Unicode Technical Standard #18
            // (https://www.unicode.org/reports/tr18/#Line_Boundaries).
            //
            // The differences are as follows:
            // 1. The XSD specification recognizes only one newline sequence - \x{A}, while Unicode Technical Standard #18
            //    defines the following newline sequences: \x{A}, \x{B}, \x{C}, \x{D}, \x{85}, \x{2028}, \x{2029}, and
            //    \x{D}\x{A} (CRLF).
            // 2. According to the XSD specification, '$' matches:
            //    - default mode: The end of the entire string.
            //    - multi-line mode: The position immediately before a newline character, and the end of the entire string if
            //      there is no newline character at the end of the string.
            //    Unicode Technical Standard #18, on the other hand, specifies that '$' matches:
            //    - default mode: The end of the entire string and before a string-ending newline.
            //    - multi-line mode: The end of the entire string and before any newline.
            if (multiLineMode) {
                // The following regex consists of two alternatives:
                // 1. (?=\x{D}\x{A}|[\x{B}-\x{D}\x{85}\x{2028}\x{2029}]|\z) - A lookahead that matches one of the following:
                //      * The CRLF sequence (\x{D}\x{A})
                //      * Any other newline character except for LF (\x{A})
                //      * The end of the string
                // 2. (?<!\x{D})(?=\x{A}) - A lookahead that matches LF (\x{A}) not preceded by CR (\x{D}).
                //      * This prevents the regex '^$' from incorrectly matching the empty string between CR and LF in a CRLF sequence,
                //        which would violate the rules defined in Unicode Technical Standard #18.
                translated.append("(?:(?=\\x{D}\\x{A}|[\\x{B}-\\x{D}\\x{85}\\x{2028}\\x{2029}]|\\z)|(?<!\\x{D})(?=\\x{A}))");
            }
            else {
                // The following regex consists of two alternatives:
                // 1. (?=\x{D}\x{A}\z|[\x{B}-\x{D}\x{85}\x{2028}\x{2029}]?\z) - A lookahead that matches one of the following:
                //      * The CRLF sequence (\x{D}\x{A}) at the end of the string
                //      * The end of the string, optionally preceded by any other newline character except LF (\x{A})
                // 2. (?<!\x{D})(?=\x{A}\z) - A lookahead that matches LF (\x{A}) at the end of the string that is not preceded by CR (\x{D}).
                //      * This prevents the regex '^$' from incorrectly matching the empty string between CR and LF in a CRLF sequence,
                //        which would violate the rules defined in Unicode Technical Standard #18.
                translated.append("(?:(?=\\x{D}\\x{A}\\z|[\\x{B}-\\x{D}\\x{85}\\x{2028}\\x{2029}]?\\z)|(?<!\\x{D})(?=\\x{A}\\z))");
            }
            return null;
        }

        @Override
        protected Void visitStartMetaCharacter(StartMetaCharacter node)
        {
            // The ISO/IEC 9075-2:2016(E) standard (section 9.22) modifies the semantics of the start-of-line/string
            // metacharacter ('^') compared to the XSD 1.0 specification (https://www.w3.org/TR/xquery-operators/#regex-syntax).
            // The ISO/IEC 9075-2:2016(E) standard derives the semantics of '^' from Unicode Technical Standard #18
            // (https://www.unicode.org/reports/tr18/#Line_Boundaries).
            //
            // The differences are as follows:
            // 1. The XSD specification recognizes only one newline sequence: \x{A}, while Unicode Technical Standard #18
            //    defines the following newline sequences: \x{A}, \x{B}, \x{C}, \x{D}, \x{85}, \x{2028}, \x{2029}, and
            //    \x{D}\x{A} (CRLF).
            // 2. According to the XSD specification, '^' matches:
            //    - default mode: The start of the entire string.
            //    - multi-line mode: The start of the entire string and the position immediately after a newline
            //      character, except when the newline is the last character in the string.
            //    Unicode Technical Standard #18, on the other hand, specifies that '^' matches:
            //    - default mode: The start of the entire string.
            //    - multi-line mode: The start of the entire string and the position immediately after any newline.
            if (multiLineMode) {
                // The following regex consists of two alternatives:
                // 1. (?<=\A|\x{D}\x{A}|[\x{A}-\x{C}\x{85}\x{2028}\x{2029}]) - A lookbehind that matches one of the following:
                //      * The start of the string
                //      * The CRLF sequence (\x{D}\x{A})
                //      * Any other newline character except for CR (\x{D})
                // 2. (?<=\x{D})(?!\x{A}) - A lookbehind that matches a CR (\x{D}) not followed by LF (\x{A}).
                //      * This prevents the regex '^$' from incorrectly matching the empty string between CR and LF in a CRLF sequence,
                //        which would violate the rules defined in Unicode Technical Standard #18.
                translated.append("(?:(?<=\\A|\\x{D}\\x{A}|[\\x{A}-\\x{C}\\x{85}\\x{2028}\\x{2029}])|(?<=\\x{D})(?!\\x{A}))");
            }
            else {
                // \A - matches the start of the string
                translated.append("\\A");
            }
            return null;
        }

        @Override
        protected Void visitCategoryEscape(CategoryEscape node)
        {
            translateCategoryEscape(node, translated);
            return null;
        }

        @Override
        protected Void visitSingleCharacterEscape(SingleCharacterEscape node)
        {
            translateSingleCharacterEscape(node, translated);
            return null;
        }

        @Override
        protected Void visitMultiCharacterEscape(MultiCharacterEscape node)
        {
            translateMultiCharacterEscape(node, translated);
            return null;
        }

        @Override
        protected Void visitSubExpression(SubExpression node)
        {
            translated.append('(');
            Iterator<Branch> iterator = node.branches().iterator();
            while (iterator.hasNext()) {
                Branch branch = iterator.next();
                process(branch);
                if (iterator.hasNext()) {
                    translated.append('|');
                }
            }
            translated.append(')');
            return null;
        }

        @Override
        protected Void visitBackReference(BackReference node)
        {
            translated.append("(?:\\");
            translated.append(node.subExpressionIndex());
            translated.append(')');
            return null;
        }
    }

    /**
     * Translates the provided character class expression to Joni's syntax.
     * The returned boolean indicates whether the character class expression includes the CRLF sequence.
     * If the returned value is true, the code using this visitor is responsible for handling the CRLF sequence,
     * as the translated character class expression does not contain it.
     */
    private static class CharacterClassExpressionTranslatingVisitor
            extends RegexTreeVisitor<Boolean>
    {
        private final StringBuilder translated = new StringBuilder();

        public String getTranslatedPattern()
        {
            return translated.toString();
        }

        @Override
        protected Boolean visitRegexNode(RegexNode node)
        {
            throw new UnsupportedOperationException("%s.visit%s is not implemented".formatted(getClass().getName(), node.getClass().getSimpleName()));
        }

        @Override
        protected Boolean visitCharacterClassExpression(CharacterClassExpression node)
        {
            boolean includesCrlf;

            translated.append('[');
            if (node.subtrahend().isPresent()) {
                // A character class subtraction is translated into an intersection ('&&') between
                // the character group and the negated subtrahend.

                if (node.negative()) {
                    // In XQuery syntax, negation takes precedence over subtraction. In Joni, intersection
                    // takes precedence over negation, so we need to wrap the negated character group in brackets.
                    translated.append("[^");
                }

                boolean characterGroupIncludesCrlf = processCharacterGroupElements(node.elements());
                if (node.negative()) {
                    translated.append(']');
                }
                translated.append("&&[^");

                boolean subtrahendIncludesCrlf = process(node.subtrahend().get());
                includesCrlf = !node.negative() && characterGroupIncludesCrlf && !subtrahendIncludesCrlf;
                translated.append(']');
            }
            else {
                if (node.negative()) {
                    translated.append('^');
                }

                boolean characterGroupIncludesCrlf = processCharacterGroupElements(node.elements());
                includesCrlf = !node.negative() && characterGroupIncludesCrlf;
            }
            translated.append(']');

            return includesCrlf;
        }

        private boolean processCharacterGroupElements(List<CharacterGroupElement> elements)
        {
            boolean includesCrlf = false;
            for (RegexNode node : elements) {
                includesCrlf |= process(node);
            }
            return includesCrlf;
        }

        @Override
        protected Boolean visitNormalCharacter(NormalCharacter node)
        {
            // Joni interprets '&&' within a character class expression as an intersection of two classes.
            // Therefore, '&' must be escaped to be treated literally.
            if (node.codePoint() == '&') {
                translated.append('\\');
            }
            translated.appendCodePoint(node.codePoint());
            return false;
        }

        @Override
        protected Boolean visitCategoryEscape(CategoryEscape node)
        {
            translateCategoryEscape(node, translated);
            return false;
        }

        @Override
        protected Boolean visitSingleCharacterEscape(SingleCharacterEscape node)
        {
            translateSingleCharacterEscape(node, translated);
            return false;
        }

        @Override
        protected Boolean visitMultiCharacterEscape(MultiCharacterEscape node)
        {
            // Sub-expressions within character class expressions are not allowed. Therefore, we include
            // only single-character sequences and leave handling of CRLF to the caller (note that the
            // method returns true for \s).
            if (node == WHITESPACE_CHARACTER_SEQUENCES) {
                translated.append("[\\x{A}-\\x{D}\\x{9}\\x{20}\\x{85}\\x{2028}\\x{2029}]");
                return true;
            }
            else {
                translateMultiCharacterEscape(node, translated);
                return false;
            }
        }

        @Override
        protected Boolean visitCharacterRange(CharacterRange node)
        {
            process(node.start());
            translated.append('-');
            process(node.end());
            return false;
        }
    }

    private static void translateMultiCharacterEscape(MultiCharacterEscape node, StringBuilder translated)
    {
        String pattern = switch (node) {
            // Per the ISO/IEC 9075-2:2016(E) standard (section 9.22), the multi-character escape "\s" matches U+0020 (space),
            // U+0009 (tab), or any single character or two-character sequence identified as a newline sequence by RL1.6
            // "Line Boundaries" in Unicode Technical Standard #18. Unicode Technical Standard #18 specifies the following
            // newline sequences: \x{A}, \x{B}, \x{C}, \x{D}, \x{85}, \x{2028}, \x{2029}, and \x{D}\x{A} (CRLF).
            //
            // To meet these requirements, we translate \s into a non-capturing group consisting of two alternatives:
            // 1. The first matches the \x{D}\x{A} (CRLF) sequence.
            // 2. The second matches any single-character sequence mentioned above that is not preceded by \x{D}\x{A}.
            //    This prevents incorrectly matching only \x{A} (LF) or \x{D} (CR) in the sequence \x{D}\x{A} during
            //    backtracking. According to Unicode Technical Standard #18, \x{D}\x{A} takes precedence over \x{D} and \x{A}.
            case WHITESPACE_CHARACTER_SEQUENCES -> "(?:\\x{D}\\x{A}|(?!\\x{D}\\x{A})[\\x{A}-\\x{D}\\x{9}\\x{20}\\x{85}\\x{2028}\\x{2029}])";
            // Per the ISO/IEC 9075-2:2016(E) standard (section 9.22), the multi-character escape "\S" matches any single
            // character that is not matched by a single character from the multi-character escape "\s". This means that \S
            // is not an exact complement of \s, and it is sufficient to negate the character class expression containing
            // all single-character sequences matched by \s.
            case ALL_EXCEPT_SINGLE_CHARACTER_WHITESPACE -> "[^\\x{A}-\\x{D}\\x{9}\\x{20}\\x{85}\\x{2028}\\x{2029}]";
            case INITIAL_NAME_CHARACTERS -> "\\p{InitialNameChar}";
            case ALL_EXCEPT_INITIAL_NAME_CHARACTERS -> "\\P{InitialNameChar}";
            case NAME_CHARACTERS -> "\\p{NameChar}";
            case ALL_EXCEPT_NAME_CHARACTERS -> "\\P{NameChar}";
            case DECIMAL_DIGITS -> "\\p{Nd}";
            case ALL_EXCEPT_DECIMAL_DIGITS -> "\\P{Nd}";
            case ALL_EXCEPT_PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS -> "[\\P{P}&&\\P{Z}&&\\P{C}]";
            case PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS -> "[\\p{P}\\p{Z}\\p{C}]";
        };
        translated.append(pattern);
    }

    private static void translateCategoryEscape(CategoryEscape node, StringBuilder translated)
    {
        String property = node.characterProperty();
        if (!isPublicCharacterProperty(property)) {
            throw new IllegalArgumentException("Invalid character property: '%s'".formatted(property));
        }
        translated.append('\\');
        translated.append(node.complement() ? 'P' : 'p');
        translated.append('{');
        translated.append(property);
        translated.append('}');
    }

    private static void translateSingleCharacterEscape(SingleCharacterEscape node, StringBuilder translated)
    {
        translated.append('\\');
        translated.appendCodePoint(node.codePoint());
    }
}
