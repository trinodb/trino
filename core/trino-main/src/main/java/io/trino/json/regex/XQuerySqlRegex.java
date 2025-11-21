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

import com.google.common.collect.ImmutableList;
import io.airlift.joni.Matcher;
import io.airlift.joni.Regex;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.json.regex.EndMetaCharacter.END_META_CHARACTER;
import static io.trino.json.regex.MultiCharacterEscape.ALL_EXCEPT_DECIMAL_DIGITS;
import static io.trino.json.regex.MultiCharacterEscape.ALL_EXCEPT_INITIAL_NAME_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.ALL_EXCEPT_NAME_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.ALL_EXCEPT_PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.ALL_EXCEPT_SINGLE_CHARACTER_WHITESPACE;
import static io.trino.json.regex.MultiCharacterEscape.DECIMAL_DIGITS;
import static io.trino.json.regex.MultiCharacterEscape.INITIAL_NAME_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.NAME_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS;
import static io.trino.json.regex.MultiCharacterEscape.WHITESPACE_CHARACTER_SEQUENCES;
import static io.trino.json.regex.Quantifier.One.ONE;
import static io.trino.json.regex.Quantifier.OneOrMore.ONE_OR_MORE;
import static io.trino.json.regex.Quantifier.ZeroOrMore.ZERO_OR_MORE;
import static io.trino.json.regex.Quantifier.ZeroOrOne.ZERO_OR_ONE;
import static io.trino.json.regex.StartMetaCharacter.START_META_CHARACTER;
import static io.trino.json.regex.WildcardEscape.WILDCARD_ESCAPE;
import static io.trino.operator.scalar.JoniRegexpFunctions.getSearchingOffset;
import static java.lang.Math.addExact;
import static java.lang.Math.multiplyExact;
import static java.util.Objects.requireNonNull;

/**
 * A compiled representation of a regular expression, conforming to the
 * <a href="https://www.w3.org/TR/xquery-operators/#regex-syntax">W3C Recommendation for XQuery 1.0 and XPath 2.0 Functions and Operators</a>,
 * with the following extensions specified in Section 9.22 of the ISO/IEC 9075-2:2016(E) standard:
 * <ul>
 *   <li>The metacharacters "^", "$", and "." are interpreted according to the guidelines of RL1.6
 *       "Line Boundaries" in the <a href="https://www.unicode.org/reports/tr18/">Unicode Technical Standard #18</a>.</li>
 *   <li>The multi-character escape "\s" matches U+0020 (space), U+0009 (tab), or any single character
 *       or two-character sequence identified as a newline sequence by RL1.6 "Line Boundaries"
 *       in the <a href="https://www.unicode.org/reports/tr18/">Unicode Technical Standard #18</a>.</li>
 *   <li>The multi-character escape "\S" matches any single character that is not matched by
 *       a single character that matches the multi-character escape "\s".</li>
 * </ul>
 *
 * <p>Note: The specific versions of XQuery and XPath, i.e., 1.0 and 2.0 respectively,
 * are referenced because the ISO/IEC 9075-2:2016(E) standard refers to the W3C Recommendation defining them.</p>
 */
public class XQuerySqlRegex
{
    // Currently, the values below are based on Joni's internal limits. Although the parser
    // implemented here aims to be independent of the regular expression engine used, enforcing
    // these limits in the parser ensures consistent behavior regardless of the underlying engine.
    private static final int MAX_RANGE_QUANTIFIER_BOUND = 100000;
    private static final int MAX_BACK_REFERENCE_INDEX = 1000;

    private final Regex regex;
    private final String pattern;
    private final Optional<String> flags;

    private XQuerySqlRegex(Regex regex, String pattern, Optional<String> flags)
    {
        this.regex = regex;
        this.pattern = pattern;
        this.flags = flags;
    }

    public static XQuerySqlRegex compile(String pattern, Optional<String> flags)
    {
        requireNonNull(pattern, "pattern is null");
        requireNonNull(flags, "flags is null");

        boolean multiLineMode = false;
        boolean dotAllMode = false;
        boolean skipWhitespaceMode = false;

        for (int flag : flagsToUnicodeCodePoints(flags)) {
            switch (flag) {
                case 'm' -> multiLineMode = true;
                case 's' -> dotAllMode = true;
                case 'x' -> skipWhitespaceMode = true;
                case 'i' -> throw new IllegalArgumentException("Flag 'i' is not yet supported");
                // Per https://www.w3.org/TR/xquery-operators/, section 7.6.1.1:
                // If there are characters present that are not defined here as flags, then an error is raised.
                default -> throw new IllegalArgumentException("Unrecognized flag: '%s'".formatted(Character.toString(flag)));
            }
        }

        PatternIterator patternIterator = new PatternIterator(pattern, skipWhitespaceMode);
        RegularExpression regularExpression = parse(patternIterator);
        Regex joniRegex = XQuerySqlToJoniTranslator.translate(regularExpression, dotAllMode, multiLineMode, pattern);
        return new XQuerySqlRegex(joniRegex, pattern, flags);
    }

    private static int[] flagsToUnicodeCodePoints(Optional<String> flags)
    {
        if (flags.isPresent()) {
            String flagsString = flags.get();
            if (!StandardCharsets.UTF_8.newEncoder().canEncode(flagsString)) {
                throw new IllegalArgumentException("The provided flag parameter is not a valid UTF-8 string");
            }
            return flagsString.codePoints().toArray();
        }
        return new int[0];
    }

    public String getPattern()
    {
        return pattern;
    }

    public Optional<String> getFlags()
    {
        return flags;
    }

    public boolean match(Slice input)
    {
        int offset = input.byteArrayOffset();
        Matcher matcher = regex.matcher(input.byteArray(), offset, offset + input.length());
        return getSearchingOffset(matcher, offset, offset + input.length()) != -1;
    }

    // The parse.*() methods below adhere to the following rules:
    // 1. Each method starts reading the pattern from the current position the iterator points to.
    //    Before invoking the method, the caller must ensure that the iterator points to the position
    //    in the pattern such that PatternIterator.currentCodePoint() returns the code point the method
    //    is supposed to read first.
    // 2. Each method leaves the iterator in a state where it points to the position immediately after
    //    the last code point read by the method.

    private static RegularExpression parse(PatternIterator pattern)
    {
        ParsingContext context = new ParsingContext();
        ImmutableList.Builder<Branch> branches = ImmutableList.builder();

        branches.add(parseBranch(pattern, context));
        while (pattern.currentCodePoint() == '|') {
            pattern.nextCodePoint();
            branches.add(parseBranch(pattern, context));
        }

        if (!pattern.noMoreCodePoints()) {
            if (pattern.currentCodePoint() == ')') {
                throw new IllegalArgumentException("Unmatched closing ')'");
            }
            throw new IllegalStateException("Internal error. Unexpected trailing characters.");
        }

        return new RegularExpression(branches.build());
    }

    private static Branch parseBranch(PatternIterator pattern, ParsingContext context)
    {
        ImmutableList.Builder<Piece> pieces = ImmutableList.builder();

        while (!pattern.noMoreCodePoints() && pattern.currentCodePoint() != '|' && pattern.currentCodePoint() != ')') {
            pieces.add(parsePiece(pattern, context));
        }

        return new Branch(pieces.build());
    }

    private static Piece parsePiece(PatternIterator pattern, ParsingContext context)
    {
        int currentCodePoint = pattern.currentCodePoint();
        Atom atom = switch (currentCodePoint) {
            case '(' -> parseSubExpression(pattern, context);
            case '[' -> parseCharacterClassExpression(pattern);
            case '.' -> {
                pattern.nextCodePoint();
                yield WILDCARD_ESCAPE;
            }
            case '\\' -> {
                int nextCodePoint = pattern.peekNextCodePoint();
                if (nextCodePoint >= '1' && nextCodePoint <= '9') {
                    yield parseBackReference(pattern, context);
                }
                yield parseCharacterClassEscape(pattern);
            }
            case '^' -> {
                pattern.nextCodePoint();
                yield START_META_CHARACTER;
            }
            case '$' -> {
                pattern.nextCodePoint();
                yield END_META_CHARACTER;
            }
            case '?', '*', '+', '{' -> throw new IllegalArgumentException("No atom before quantifier '%s'".formatted((char) currentCodePoint));
            case '}', ']' -> throw new IllegalArgumentException("Unescaped metacharacter '%s'".formatted((char) currentCodePoint));
            case '|' -> throw new IllegalStateException("Internal error. Unexpected '|'.");
            default -> {
                pattern.nextCodePoint();
                yield new NormalCharacter(currentCodePoint);
            }
        };

        Quantifier quantifier = parseQuantifier(pattern);

        return new Piece(atom, quantifier);
    }

    private static BackReference parseBackReference(PatternIterator pattern, ParsingContext context)
    {
        int currentCodePoint = pattern.nextCodePoint(); // skip '\'
        int subExpressionIndex = currentCodePoint - '0';

        currentCodePoint = pattern.nextCodePoint();
        while (currentCodePoint >= '0' && currentCodePoint <= '9') {
            int subExpressionIndexCandidate;
            int digit = currentCodePoint - '0';
            try {
                subExpressionIndexCandidate = addExact(multiplyExact(subExpressionIndex, 10), digit);
            }
            catch (ArithmeticException e) {
                throw new IllegalArgumentException("Sub-expression index used in back-reference must not exceed " + MAX_BACK_REFERENCE_INDEX);
            }
            if (subExpressionIndexCandidate > context.openSubExpressionCount()) {
                break;
            }
            subExpressionIndex = subExpressionIndexCandidate;
            currentCodePoint = pattern.nextCodePoint();
        }

        if (subExpressionIndex > MAX_BACK_REFERENCE_INDEX) {
            throw new IllegalArgumentException("Sub-expression index used in back-reference must not exceed " + MAX_BACK_REFERENCE_INDEX);
        }
        if (!context.closedSubExpressionExists(subExpressionIndex)) {
            throw new IllegalArgumentException("Invalid back-reference. Sub-expression with index " + subExpressionIndex + " either does not exist or is not yet closed.");
        }

        return new BackReference(subExpressionIndex);
    }

    private static Quantifier parseQuantifier(PatternIterator pattern)
    {
        Quantifier quantifier = switch (pattern.currentCodePoint()) {
            case '?' -> {
                pattern.nextCodePoint();
                yield ZERO_OR_ONE;
            }
            case '*' -> {
                pattern.nextCodePoint();
                yield ZERO_OR_MORE;
            }
            case '+' -> {
                pattern.nextCodePoint();
                yield ONE_OR_MORE;
            }
            case '{' -> parseRangeQuantifier(pattern);
            default -> ONE;
        };

        if (pattern.currentCodePoint() == '?') {
            // Per the XSD 1.0 specification:
            // Reluctant quantifiers have no effect on the results of the boolean fn:matches function,
            // since this function is only interested in discovering whether a match exists, and not
            // where it exists.
            pattern.nextCodePoint();
        }

        return quantifier;
    }

    private static Quantifier parseRangeQuantifier(PatternIterator pattern)
    {
        pattern.nextCodePoint(); // skip '{'

        Quantifier rangeQuantifier;
        int min = parseRangeQuantifierBound(pattern);
        if (pattern.currentCodePoint() == ',') {
            if (pattern.nextCodePoint() != '}') {
                int max = parseRangeQuantifierBound(pattern);
                if (min > max) {
                    throw new IllegalArgumentException("Invalid quantifier. Expected {n,m}, {n}, or {n,}, where n and m are non-negative integers, and n <= m.");
                }
                rangeQuantifier = new Quantifier.ExplicitRange(min, max);
            }
            else {
                rangeQuantifier = new Quantifier.AtLeast(min);
            }
        }
        else {
            rangeQuantifier = new Quantifier.Exact(min);
        }

        if (pattern.currentCodePoint() != '}') {
            throw new IllegalArgumentException("Invalid quantifier. Expected {n,m}, {n}, or {n,}, where n and m are non-negative integers, and n <= m.");
        }
        pattern.nextCodePoint();

        return rangeQuantifier;
    }

    private static int parseRangeQuantifierBound(PatternIterator pattern)
    {
        int bound = 0;
        boolean atLeastOneDigit = false;

        int currentCodePoint = pattern.currentCodePoint();
        while (currentCodePoint >= '0' && currentCodePoint <= '9') {
            atLeastOneDigit = true;

            int digit = currentCodePoint - '0';
            try {
                bound = addExact(multiplyExact(bound, 10), digit);
            }
            catch (ArithmeticException e) {
                throw new IllegalArgumentException("Range quantifier bounds must not exceed " + MAX_RANGE_QUANTIFIER_BOUND);
            }

            currentCodePoint = pattern.nextCodePoint();
        }

        if (!atLeastOneDigit) {
            throw new IllegalArgumentException("Invalid quantifier. Expected {n,m}, {n}, or {n,}, where n and m are non-negative integers, and n <= m.");
        }
        if (bound > MAX_RANGE_QUANTIFIER_BOUND) {
            throw new IllegalArgumentException("Range quantifier bounds must not exceed " + MAX_RANGE_QUANTIFIER_BOUND);
        }

        return bound;
    }

    private static SubExpression parseSubExpression(PatternIterator pattern, ParsingContext context)
    {
        ImmutableList.Builder<Branch> branches = ImmutableList.builder();

        int subExpressionIndex = context.recordOpenSubExpression();

        do {
            pattern.nextCodePoint();
            branches.add(parseBranch(pattern, context));
        }
        while (pattern.currentCodePoint() == '|');

        if (pattern.currentCodePoint() != ')') {
            throw new IllegalArgumentException("Unclosed sub-expression");
        }
        context.recordClosedSubExpression(subExpressionIndex);
        pattern.nextCodePoint(); // skip ')'

        return new SubExpression(branches.build());
    }

    private static CharacterClassEscape parseCharacterClassEscape(PatternIterator pattern)
    {
        int currentCodePoint = pattern.nextCodePoint();
        if (pattern.noMoreCodePoints()) {
            throw new IllegalArgumentException("Unescaped trailing '\\'");
        }
        CharacterClassEscape escape = switch (currentCodePoint) {
            case 'n', 'r', 't', '\\', '|', '.', '?', '*', '+', '(', ')', '{', '}', '$', '-', '[', ']', '^' -> new SingleCharacterEscape(currentCodePoint);
            case 'c' -> NAME_CHARACTERS;
            case 'C' -> ALL_EXCEPT_NAME_CHARACTERS;
            case 'i' -> INITIAL_NAME_CHARACTERS;
            case 'I' -> ALL_EXCEPT_INITIAL_NAME_CHARACTERS;
            case 's' -> WHITESPACE_CHARACTER_SEQUENCES;
            case 'S' -> ALL_EXCEPT_SINGLE_CHARACTER_WHITESPACE;
            case 'd' -> DECIMAL_DIGITS;
            case 'D' -> ALL_EXCEPT_DECIMAL_DIGITS;
            case 'w' -> ALL_EXCEPT_PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS;
            case 'W' -> PUNCTUATION_SEPARATOR_AND_OTHER_CHARACTERS;
            case 'p', 'P' -> {
                boolean complement = currentCodePoint == 'P';
                currentCodePoint = pattern.nextCodePoint();

                if (currentCodePoint != '{') {
                    throw new IllegalArgumentException("Invalid escape sequence. Expected '{' after '\\%s'.".formatted(complement ? 'P' : 'p'));
                }
                pattern.nextCodePoint();

                String characterProperty = parseCharacterProperty(pattern);

                if (pattern.currentCodePoint() != '}') {
                    throw new IllegalArgumentException("Unclosed escape sequence. Expected '}' after '%s'.".formatted(characterProperty));
                }
                yield new CategoryEscape(characterProperty, complement);
            }
            default -> throw new IllegalArgumentException("Unexpected escape sequence: '\\%s'".formatted(Character.toString(currentCodePoint)));
        };
        pattern.nextCodePoint();

        return escape;
    }

    private static String parseCharacterProperty(PatternIterator pattern)
    {
        StringBuilder property = new StringBuilder();

        int currentCodePoint = pattern.currentCodePoint();
        while (!pattern.noMoreCodePoints() && currentCodePoint != '}') {
            boolean validCharacter = (currentCodePoint >= 'a' && currentCodePoint <= 'z')
                    || (currentCodePoint >= 'A' && currentCodePoint <= 'Z')
                    || (currentCodePoint >= '0' && currentCodePoint <= '9')
                    || (currentCodePoint == '-');
            if (validCharacter) {
                property.appendCodePoint(currentCodePoint);
            }
            else {
                throw new IllegalArgumentException("Invalid character property. Unexpected character '%s'.".formatted(Character.toString(currentCodePoint)));
            }
            currentCodePoint = pattern.nextCodePoint();
        }

        return property.toString();
    }

    private static CharacterClassExpression parseCharacterClassExpression(PatternIterator pattern)
    {
        pattern.disableWhitespaceSkipping();
        int currentCodePoint = pattern.nextCodePoint(); // skip '['

        boolean negativeGroup = false;
        if (currentCodePoint == '^') {
            negativeGroup = true;
            currentCodePoint = pattern.nextCodePoint();
        }

        CharacterGroupElementsBuilder groupElementsBuilder = new CharacterGroupElementsBuilder();
        Optional<CharacterClassExpression> subtrahend = Optional.empty();

        boolean firstCharacter = true;
        while (!pattern.noMoreCodePoints() && currentCodePoint != ']') {
            switch (currentCodePoint) {
                case '[' -> throw new IllegalArgumentException("Unescaped '[' within character class expression");
                case '\\' -> {
                    groupElementsBuilder.add(parseCharacterClassEscape(pattern));
                    currentCodePoint = pattern.currentCodePoint();
                }
                case '-' -> {
                    currentCodePoint = pattern.nextCodePoint();
                    if (currentCodePoint == ']' || (currentCodePoint == '-' && pattern.peekNextCodePoint() == '[')) {
                        groupElementsBuilder.add(new NormalCharacter('-'));
                    }
                    else if (currentCodePoint == '[') {
                        CharacterClassExpression characterClassExpression = parseCharacterClassExpression(pattern);
                        subtrahend = Optional.of(characterClassExpression);
                        currentCodePoint = pattern.currentCodePoint();
                        if (currentCodePoint != ']') {
                            throw new IllegalArgumentException("Expected ']' after subtraction");
                        }
                    }
                    else if (firstCharacter) {
                        groupElementsBuilder.add(new NormalCharacter('-'));
                    }
                    else {
                        groupElementsBuilder.useLastElementAsRangeStart();
                    }
                }
                default -> {
                    groupElementsBuilder.add(new NormalCharacter(currentCodePoint));
                    currentCodePoint = pattern.nextCodePoint();
                }
            }
            firstCharacter = false;
        }

        if (currentCodePoint != ']') {
            throw new IllegalArgumentException("Unclosed character class expression");
        }

        pattern.enableWhitespaceSkipping();
        pattern.nextCodePoint(); // skip ']'

        return new CharacterClassExpression(negativeGroup, groupElementsBuilder.build(), subtrahend);
    }

    private static class CharacterGroupElementsBuilder
    {
        private final Deque<CharacterGroupElement> elements = new ArrayDeque<>();
        private boolean inRange;

        public void add(CharacterClassEscape escape)
        {
            if (escape instanceof SingleCharacterEscape singleCharacterEscape) {
                if (inRange) {
                    finishRange(singleCharacterEscape);
                }
                else {
                    elements.push(escape);
                }
            }
            else if (inRange) {
                throw new IllegalArgumentException("Multi-character escape cannot follow '-'");
            }
            else {
                elements.push(escape);
            }
        }

        public void add(NormalCharacter character)
        {
            if (inRange) {
                if (character.codePoint() == '-') {
                    throw new IllegalArgumentException("Unescaped '-' cannot act as end of range");
                }
                finishRange(character);
            }
            else {
                elements.push(character);
            }
        }

        private void finishRange(CharacterRangeBound end)
        {
            checkState(!elements.isEmpty(), "Internal error. Character group elements stack is empty.");
            CharacterGroupElement lastElement = elements.pop();
            if (lastElement instanceof NormalCharacter(int codePoint)) {
                if (codePoint == '-') {
                    throw new IllegalArgumentException("Unescaped '-' cannot act as start of range");
                }
            }
            if (lastElement instanceof CharacterRangeBound start) {
                if (start.effectiveCodePoint() > end.effectiveCodePoint()) {
                    throw new IllegalArgumentException("Invalid character range: start > end");
                }
                elements.push(new CharacterRange(start, end));
            }
            else {
                throw new IllegalArgumentException("Unescaped '-' within character class expression");
            }
            inRange = false;
        }

        public void useLastElementAsRangeStart()
        {
            if (inRange) {
                throw new IllegalArgumentException("Unescaped '-' within character class expression");
            }
            inRange = true;
        }

        public List<CharacterGroupElement> build()
        {
            checkState(!inRange, "Internal error. Unfinished range.");
            if (elements.isEmpty()) {
                throw new IllegalArgumentException("Character group must not be empty");
            }
            return ImmutableList.copyOf(elements.descendingIterator());
        }
    }

    private static class PatternIterator
    {
        private static final int END = -1;

        private final int[] codePoints;
        private final boolean skipWhitespaceMode;

        private int index = -1;
        private int currentCodePoint;
        private boolean skipWhitespace;

        public PatternIterator(String pattern, boolean skipWhitespaceMode)
        {
            if (!StandardCharsets.UTF_8.newEncoder().canEncode(pattern)) {
                throw new IllegalArgumentException("The provided pattern is not a valid UTF-8 string");
            }
            this.codePoints = pattern.codePoints().toArray();
            this.skipWhitespaceMode = skipWhitespaceMode;
            this.skipWhitespace = skipWhitespaceMode;
            nextCodePoint();
        }

        public boolean noMoreCodePoints()
        {
            return currentCodePoint == END;
        }

        /**
         * Returns the current code point or {@code -1} if the iterator has moved past the end of the pattern.
         */
        public int currentCodePoint()
        {
            return currentCodePoint;
        }

        /**
         * Peeks at the next code point without advancing the iterator. Respects the whitespace skipping setting.
         * If the iterator has moved past the end of the pattern, it returns {@code -1}.
         */
        public int peekNextCodePoint()
        {
            int currentIndex = index;
            if (skipWhitespace) {
                while (currentIndex + 1 < codePoints.length && isWhitespace(codePoints[currentIndex + 1])) {
                    currentIndex++;
                }
            }
            if (currentIndex + 1 >= codePoints.length) {
                return END;
            }
            else {
                return codePoints[currentIndex + 1];
            }
        }

        /**
         * Advances the iterator to the next code point and returns it. Respects the whitespace skipping setting.
         * If the iterator has moved past the end of the pattern, it returns {@code -1}.
         */
        public int nextCodePoint()
        {
            if (skipWhitespace) {
                while (index + 1 < codePoints.length && isWhitespace(codePoints[index + 1])) {
                    index++;
                }
            }
            if (index + 1 >= codePoints.length) {
                currentCodePoint = END;
            }
            else {
                currentCodePoint = codePoints[++index];
            }
            return currentCodePoint;
        }

        private static boolean isWhitespace(int codePoint)
        {
            return codePoint == '\t' || codePoint == '\n' || codePoint == '\r' || codePoint == ' ';
        }

        public void disableWhitespaceSkipping()
        {
            skipWhitespace = false;
        }

        public void enableWhitespaceSkipping()
        {
            skipWhitespace = skipWhitespaceMode;
        }
    }

    private static class ParsingContext
    {
        private int openSubExpressionCount;
        private final IntSet closedSubExpressions = new IntOpenHashSet();

        public int recordOpenSubExpression()
        {
            return ++openSubExpressionCount;
        }

        public int openSubExpressionCount()
        {
            return openSubExpressionCount;
        }

        public void recordClosedSubExpression(int subExpressionIndex)
        {
            closedSubExpressions.add(subExpressionIndex);
        }

        public boolean closedSubExpressionExists(int subExpressionIndex)
        {
            return closedSubExpressions.contains(subExpressionIndex);
        }
    }
}
