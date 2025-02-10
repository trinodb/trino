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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.IntStream;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.airlift.slice.Slices.EMPTY_SLICE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Character.MAX_CODE_POINT;
import static java.lang.Character.MAX_SURROGATE;
import static java.lang.Character.MIN_CODE_POINT;
import static java.lang.Character.MIN_SURROGATE;
import static java.util.Objects.requireNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestXQuerySqlRegex
{
    private static final List<UnicodeCharacter> UNICODE_CHARACTERS = loadUnicodeCharacters();
    private static final Set<String> LINE_SEPARATORS = ImmutableSet.of("\n", "\u000B", "\u000C", "\r", "\u0085", "\u2028", "\u2029", "\r\n");

    @Test
    void testBranches()
    {
        // one branch
        assertMatches("abc", "abc");
        assertMatches("aabcaa", "abc");
        assertDoesNotMatch("aaa", "abc");
        assertMatches("\uD83C\uDC00£ਞ", "\uD83C\uDC00£ਞ");

        // multiple branches
        assertMatches("a", "a|b");
        assertMatches("b", "a|b");
        assertMatches("ca", "a|b");
        assertDoesNotMatch("c", "a|b");
        assertMatches("c", "a|b|c");
        assertMatches("\uD83C\uDC00", "\uD83C\uDC00|£|ਞ");
        assertMatches("£", "\uD83C\uDC00|£|ਞ");
        assertMatches("ਞ", "\uD83C\uDC00|£|ਞ");

        // empty branch
        assertMatches("", "");
        assertMatches("a", "");
        assertMatches("", "|b");
        assertMatches("a", "|b");
        assertMatches("b", "|b");
        assertMatches("", "b|");
        assertMatches("a", "b|");
        assertMatches("b", "b|");
        assertMatches("", "a||b");
        assertMatches("a", "a||b");
        assertMatches("b", "a||b");
    }

    @Test
    void testQuantifiers()
    {
        // zero or one
        assertMatches("a", "a?");
        assertMatches("", "a?");
        assertMatches("aaa", "a?");
        assertMatches("b", "a?");
        assertMatches("abc", "a?");
        assertMatches("\uD83C\uDC00", "\uD83C\uDC00?");

        // zero or more
        assertMatches("a", "a*");
        assertMatches("", "a*");
        assertMatches("aaa", "a*");
        assertMatches("b", "a*");
        assertMatches("bac", "a*");
        assertMatches("\uD83C\uDC00", "\uD83C\uDC00*");

        // one or more
        assertMatches("a", "a+");
        assertMatches("aaa", "a+");
        assertMatches("bac", "a+");
        assertDoesNotMatch("", "a+");
        assertDoesNotMatch("b", "a+");
        assertDoesNotMatch("", "\uD83C\uDC00+");
        assertMatches("\uD83C\uDC00", "\uD83C\uDC00+");

        // range
        assertDoesNotMatch("", "a{2,5}");
        assertDoesNotMatch("a", "a{2,5}");
        assertMatches("aa", "a{2,5}");
        assertMatches("aaa", "a{2,5}");
        assertMatches("aaaa", "a{2,5}");
        assertMatches("aaaaa", "a{2,5}");
        assertMatches("aaaaaa", "a{2,5}");
        assertDoesNotMatch("b", "a{2,5}");
        assertDoesNotMatch("\uD83C\uDC00", "\uD83C\uDC00{2,5}");
        assertMatches("\uD83C\uDC00".repeat(2), "\uD83C\uDC00{2,5}");

        // exact count
        assertDoesNotMatch("", "a{4}");
        assertDoesNotMatch("a", "a{4}");
        assertDoesNotMatch("aa", "a{4}");
        assertDoesNotMatch("aaa", "a{4}");
        assertMatches("aaaa", "a{4}");
        assertMatches("aaaaa", "a{4}");
        assertMatches("aaaa", "^a{4}$");
        assertDoesNotMatch("aaaaa", "^a{4}$");
        assertDoesNotMatch("b", "a{4}");
        assertMatches("\uD83C\uDC00".repeat(4), "\uD83C\uDC00{4}");
        assertDoesNotMatch("\uD83C\uDC00".repeat(3), "\uD83C\uDC00{4}");
        assertDoesNotMatch("\uD83C\uDC00".repeat(5), "^\uD83C\uDC00{4}$");

        // at least
        assertDoesNotMatch("", "a{3,}");
        assertDoesNotMatch("a", "a{3,}");
        assertDoesNotMatch("aa", "a{3,}");
        assertMatches("aaa", "a{3,}");
        assertMatches("aaaa", "a{3,}");
        assertDoesNotMatch("b", "a{3,}");
        assertMatches("\uD83C\uDC00".repeat(3), "^\uD83C\uDC00{3,}$");
        assertDoesNotMatch("\uD83C\uDC00".repeat(2), "^\uD83C\uDC00{3,}$");

        // at most
        assertMatches("", "a{0,3}");
        assertMatches("a", "a{0,3}");
        assertMatches("aa", "a{0,3}");
        assertMatches("aaa", "a{0,3}");
        assertMatches("aaaa", "a{0,3}");
        assertMatches("b", "a{0,3}");
        assertMatches("\uD83C\uDC00".repeat(3), "^\uD83C\uDC00{0,3}$");
        assertDoesNotMatch("\uD83C\uDC00".repeat(5), "^\uD83C\uDC00{0,3}$");

        // empty
        assertMatches("", "a{0,0}");
        assertMatches("a", "a{0,0}");
        assertMatches("b", "a{0,0}");
        assertMatches("", "\uD83C\uDC00{0,0}");
    }

    @Test
    void testDanglingQuantifier()
    {
        assertInvalid("?", "No atom before quantifier '?'");
        assertInvalid("a???", "No atom before quantifier '?'");
        assertInvalid("*", "No atom before quantifier '*'");
        assertInvalid("a**", "No atom before quantifier '*'");
        assertInvalid("a?*", "No atom before quantifier '*'");
        assertInvalid("+", "No atom before quantifier '+'");
        assertInvalid("a++", "No atom before quantifier '+'");
        assertInvalid("a{0,4}+", "No atom before quantifier '+'");
        assertInvalid("a?+", "No atom before quantifier '+'");
        assertInvalid("{", "No atom before quantifier '{'");
        assertInvalid("{0,4}", "No atom before quantifier '{'");
    }

    @Test
    void testInvalidRangeQuantifier()
    {
        String errorMessage = "Invalid quantifier. Expected {n,m}, {n}, or {n,}, where n and m are non-negative integers, and n <= m.";
        assertInvalid("a{,3}", errorMessage);
        assertInvalid("a{a,}", errorMessage);
        assertInvalid("a{a}", errorMessage);
        assertInvalid("a{1,a}", errorMessage);
        assertInvalid("a{-1,3}", errorMessage);
        assertInvalid("a{-3,-1}", errorMessage);
        assertInvalid("a{", errorMessage);
        assertInvalid("a{1,", errorMessage);
        assertInvalid("a{1", errorMessage);
        assertInvalid("a{,}", errorMessage);
        assertInvalid("a{}", errorMessage);
        assertInvalid("a{1, 3}", errorMessage);
        assertInvalid("a{ 1,3}", errorMessage);
        assertInvalid("a{1,3 }", errorMessage);
        assertInvalid("a{3,1}", errorMessage);
    }

    @Test
    void testRangeQuantifierBounds()
    {
        assertMatches("a".repeat(100000), "^a{0,100000}$");
        assertMatches("a".repeat(100000), "^a{100000}$");
        assertMatches("a".repeat(100000), "^a{100000,100000}$");

        assertInvalid("^a{0,100001}$", "Range quantifier bounds must not exceed 100000");
        assertInvalid("^a{100001}$", "Range quantifier bounds must not exceed 100000");
        assertInvalid("^a{100001,100001}$", "Range quantifier bounds must not exceed 100000");

        // verify that overflow does not occur
        assertInvalid("^a{0," + Long.MAX_VALUE + "}$", "Range quantifier bounds must not exceed 100000");
        assertInvalid("^a{" + Long.MAX_VALUE + "}$", "Range quantifier bounds must not exceed 100000");
        assertInvalid("^a{" + Long.MAX_VALUE + "," + Long.MAX_VALUE + "}$", "Range quantifier bounds must not exceed 100000");
    }

    @Test
    void testReluctantQuantifiers()
    {
        // zero or one
        assertMatches("a", "a??");
        assertMatches("", "a??");
        assertMatches("aaa", "a??");
        assertMatches("b", "a??");
        assertMatches("abc", "a??");
        assertMatches("bb", "ba??b");
        assertMatches("bab", "ba??b");
        assertDoesNotMatch("baab", "ba??b");

        // zero or more
        assertMatches("a", "a*?");
        assertMatches("", "a*?");
        assertMatches("aaa", "a*?");
        assertMatches("b", "a*?");
        assertMatches("bac", "a*?");
        assertMatches("bb", "ba*?b");
        assertMatches("bab", "ba*?b");
        assertMatches("baaab", "ba*?b");
        assertDoesNotMatch("baacb", "ba*?b");

        // one or more
        assertMatches("a", "a+?");
        assertMatches("aaa", "a+?");
        assertMatches("bac", "a+?");
        assertDoesNotMatch("", "a+?");
        assertDoesNotMatch("b", "a+?");
        assertDoesNotMatch("bb", "ba+?b");
        assertMatches("bab", "ba+?b");
        assertMatches("baaab", "ba+?b");

        // range
        assertDoesNotMatch("", "a{2,5}?");
        assertDoesNotMatch("a", "a{2,5}?");
        assertMatches("aa", "a{2,5}?");
        assertMatches("aaa", "a{2,5}?");
        assertMatches("aaaa", "a{2,5}?");
        assertMatches("aaaaa", "a{2,5}?");
        assertMatches("aaaaaa", "a{2,5}?");
        assertDoesNotMatch("b", "a{2,5}?");
        assertDoesNotMatch("bab", "ba{2,5}?b");
        assertMatches("baab", "ba{2,5}?b");
        assertMatches("baaaaab", "ba{2,5}?b");
        assertDoesNotMatch("baaaaaab", "ba{2,5}?b");

        // exact count
        assertDoesNotMatch("", "a{4}?");
        assertDoesNotMatch("a", "a{4}?");
        assertDoesNotMatch("aa", "a{4}?");
        assertDoesNotMatch("aaa", "a{4}?");
        assertMatches("aaaa", "a{4}?");
        assertMatches("aaaaa", "a{4}?");
        assertDoesNotMatch("b", "a{4}?");
        assertDoesNotMatch("bab", "ba{4}?b");
        assertMatches("baaaab", "ba{4}?b");
        assertDoesNotMatch("baaaaab", "ba{4}?b");

        // at least
        assertDoesNotMatch("", "a{3,}?");
        assertDoesNotMatch("a", "a{3,}?");
        assertDoesNotMatch("aa", "a{3,}?");
        assertMatches("aaa", "a{3,}?");
        assertMatches("aaaa", "a{3,}?");
        assertDoesNotMatch("b", "a{3,}?");
        assertDoesNotMatch("baab", "ba{3,}?b");
        assertMatches("baaab", "ba{3,}?b");
        assertMatches("baaaaab", "ba{3,}?b");

        // at most
        assertMatches("", "a{0,3}?");
        assertMatches("a", "a{0,3}?");
        assertMatches("aa", "a{0,3}?");
        assertMatches("aaa", "a{0,3}?");
        assertMatches("aaaa", "a{0,3}?");
        assertMatches("b", "a{0,3}?");
        assertMatches("bb", "ba{0,3}?b");
        assertMatches("baaab", "ba{0,3}?b");
        assertDoesNotMatch("baaaab", "ba{0,3}?b");

        // empty
        assertMatches("", "a{0,0}?");
        assertMatches("a", "a{0,0}?");
        assertMatches("b", "a{0,0}?");
        assertDoesNotMatch("bab", "ba{0,0}?b");
        assertMatches("bb", "ba{0,0}?b");
    }

    @Test
    void testUnescapedMetacharacters()
    {
        assertInvalid("}", "Unescaped metacharacter '}'");
        assertInvalid("0,1}", "Unescaped metacharacter '}'");
        assertInvalid("a{1}}", "Unescaped metacharacter '}'");
        assertInvalid("]", "Unescaped metacharacter ']'");
        assertInvalid("[a-z]]", "Unescaped metacharacter ']'");
    }

    @Test
    void testSubRegularExpression()
    {
        assertInvalid(")", "Unmatched closing ')'");
        assertInvalid("\\()", "Unmatched closing ')'");
        assertInvalid("(", "Unclosed sub-expression");
        assertInvalid("(\\)", "Unclosed sub-expression");

        assertMatches("", "()");
        assertMatches("abc", "(abc)");
        assertMatches("abc", "(ab)(c)");
        assertMatches("abc", "((ab)(c))");
        assertMatches("\uD83C\uDC00£ਞ", "((\uD83C\uDC00)(£ਞ))");

        // non-capturing groups are not supported
        assertInvalid("(?:abc)", "No atom before quantifier '?'");
        assertMatches("?:abc", "(\\?:abc)");

        // lookahead is not supported
        assertInvalid("q(?=u)", "No atom before quantifier '?'");
        assertMatches("q?=u", "q(\\?=u)");
        assertInvalid("q(?!u)", "No atom before quantifier '?'");
        assertMatches("q?!u", "q(\\?!u)");

        // lookbehind is not supported
        assertInvalid("(?<=q)u", "No atom before quantifier '?'");
        assertMatches("?<=qu", "(\\?<=q)u");
        assertInvalid("(?<!q)u", "No atom before quantifier '?'");
        assertMatches("?<!qu", "(\\?<!q)u");
    }

    @Test
    void testBackReferences()
    {
        // single-digit back-reference
        assertMatches("aa", "(a)\\1");
        assertMatches("aa1", "(a)\\11");
        assertMatches("abcdefghiabcdefghia0a1", "(a)(b)(c)(d)(e)(f)(g)(h)(i)\\1\\2\\3\\4\\5\\6\\7\\8\\9\\10\\11");

        // multi-digit back-reference
        assertMatches("abcdefghijkabcdefghijk", "(a)(b)(c)(d)(e)(f)(g)(h)(i)(j)(k)\\1\\2\\3\\4\\5\\6\\7\\8\\9\\10\\11");
        assertMatches("abcdefghia0jj", "(a)(b)(c)(d)(e)(f)(g)(h)(i)\\10(j)\\10");
        assertMatches("a".repeat(101), "^" + "(a)".repeat(100) + "\\100$");

        // nested sub-expression
        assertMatches("bab", "((b)a\\2)");
        assertMatches("babbab", "^((b)a\\2)\\1$");
        assertMatches("bccabcc", "^((b(c)\\3)a\\2)$");

        // multi-character escape in back-referenced sub-expression
        assertDoesNotMatch("zabcx", "(\\c)abc\\1");
        assertMatches("zabcz", "(\\c)abc\\1");
        assertMatches("3abc3", "(\\d)abc\\1");
        assertDoesNotMatch("3abc1", "(\\d)abc\\1");
        assertDoesNotMatch("\nabc\r", "(\\s)abc\\1");
        assertMatches("\nabc\n", "(\\s)abc\\1");
        assertDoesNotMatch("\r\nabc\r", "(\\s)abc\\1");
        assertDoesNotMatch("\r\nabc\r", "(\\s)abc\\1");
        assertMatches("\r\nabc\r\n", "(\\s)abc\\1");
        assertDoesNotMatch("\r\nabc\r", "([\\s])abc\\1");
        assertDoesNotMatch("\r\nabc\r", "([\\s])abc\\1");
        assertMatches("\r\nabc\r\n", "([\\s])abc\\1");
        assertMatches("\r\nabc\n", "(\\s)abc\\1"); // \s can match either \r\n or \n
        assertMatches("\r\nabc\n", "([\\s])abc\\1"); // \s can match either \r\n or \n
        assertDoesNotMatch("\r\nabc\n", "^(\\s)abc\\1$"); // \s can only match \r\n
        assertDoesNotMatch("\r\nabc\n", "^([\\s])abc\\1$"); // \s can only match \r\n
        assertMatches("\n\nabc\n", "(\\s)(\\s)abc\\2");
        assertMatches("\n\nabc\n", "([\\s])([\\s])abc\\2");
        assertDoesNotMatch("\r\nabc\n", "(\\s)(\\s)abc\\2");
        assertDoesNotMatch("\r\nabc\n", "([\\s])([\\s])abc\\2");

        // based on the example from https://www.w3.org/TR/xquery-operators/#regex-syntax
        assertMatches("'aaa'", "('|\").*\\1");
        assertMatches("\"aaa\"", "('|\").*\\1");
        assertDoesNotMatch("aaa", "('|\").*\\1");
        assertDoesNotMatch("'aaa\"", "('|\").*\\1");
        assertDoesNotMatch("\"aaa'", "('|\").*\\1");
        assertDoesNotMatch("'aaa'", "('|\").*\\11");

        // escaped parentheses
        assertMatches("(b)aa", "\\(b\\)(a)\\1");
        assertMatches("(ba)a", "\\(b(a)\\)\\1");

        // wildcard escape
        assertMatches("aba", "(.)b\\1");
        assertDoesNotMatch("\rb\r", "(.)b\\1");
        assertMatches("\rb\r", "(.)b\\1", "s");
        assertDoesNotMatch("\r\nb\r", "(.)b\\1", "s");
        assertMatches("\r\nb\r\n", "(.)b\\1", "s");
        assertMatches("\r\nb\n", "(.)b\\1", "s");
    }

    @Test
    void testBackReferenceLimit()
    {
        assertMatches("a".repeat(999) + "bb", "^" + "(a|b)".repeat(1000) + "\\1000$");
        assertInvalid("^" + "(a|b)".repeat(1001) + "\\1001$", "Sub-expression index used in back-reference must not exceed 1000");
    }

    @Test
    void testInvalidBackReference()
    {
        assertInvalid("(a)\\0", "Unexpected escape sequence: '\\0'");
        assertInvalid("(a\\1)", "Invalid back-reference. Sub-expression with index 1 either does not exist or is not yet closed.");
        assertInvalid("\\(a\\)\\1", "Invalid back-reference. Sub-expression with index 1 either does not exist or is not yet closed.");
        assertInvalid("(a)(b)(c)(d)(e)(f)(g)(h)(i)(j\\10)", "Invalid back-reference. Sub-expression with index 10 either does not exist or is not yet closed.");
        assertInvalid("('|\").*\\9", "Invalid back-reference. Sub-expression with index 9 either does not exist or is not yet closed.");
        assertInvalid("('|\").*[\\1]", "Unexpected escape sequence: '\\1'");
    }

    @Test
    void testUnrecognizedFlag()
    {
        assertInvalid("abc", "S", "Unrecognized flag: 'S'");
        assertInvalid("abc", "z", "Unrecognized flag: 'z'");
        assertInvalid("abc", "\uD840\uDC40", "Unrecognized flag: '\uD840\uDC40'");
    }

    @Test
    void testMultipleFlags()
    {
        assertMatches("\n", ".", "ss");
        assertMatches("x\ny\nabc\nz", "^abc", "mm");
        assertMatches("abc\n123", "^abc$.^123$", "ms");
        assertMatches("abc\n123", "^abc$.^123$", "sm");
    }

    @Test
    void testSingleCharacterEscape()
    {
        assertMatches("a\nb", "a\\nb");
        assertMatches("a\rb", "a\\rb");
        assertMatches("a\tb", "a\\tb");
        assertMatches("a\\b", "a\\\\b");
        assertMatches("a|b", "a\\|b");
        assertMatches("a.b", "a\\.b");
        assertMatches("a-b", "a\\-b");
        assertMatches("a^b", "a\\^b");
        assertMatches("a?b", "a\\?b");
        assertMatches("a*b", "a\\*b");
        assertMatches("a+b", "a\\+b");
        assertMatches("a{b", "a\\{b");
        assertMatches("a}b", "a\\}b");
        assertMatches("a(b", "a\\(b");
        assertMatches("a)b", "a\\)b");
        assertMatches("a[b", "a\\[b");
        assertMatches("a]b", "a\\]b");
        assertMatches("a$b", "a\\$b");
    }

    @Test
    void testInvalidEscape()
    {
        assertInvalid("\\", "Unescaped trailing '\\'");
        // Make sure that the following escape sequences supported by some regex syntaxes are illegal:
        assertInvalid("\\a", "Unexpected escape sequence: '\\a'");
        assertInvalid("\\b", "Unexpected escape sequence: '\\b'");
        assertInvalid("\\e", "Unexpected escape sequence: '\\e'");
        assertInvalid("\\f", "Unexpected escape sequence: '\\f'");
        assertInvalid("\\v", "Unexpected escape sequence: '\\v'");
        assertInvalid("\\A", "Unexpected escape sequence: '\\A'");
        assertInvalid("\\Z", "Unexpected escape sequence: '\\Z'");
        assertInvalid("\\z", "Unexpected escape sequence: '\\z'");
        // Make sure that hexadecimal number escapes are illegal:
        assertInvalid("\\x{2029}", "Unexpected escape sequence: '\\x'");
        assertInvalid("[\\x{2029}]", "Unexpected escape sequence: '\\x'");
    }

    @Test
    void testInvalidCategoryEscape()
    {
        testInvalidCategoryEscape('p');
        testInvalidCategoryEscape('P');
    }

    private void testInvalidCategoryEscape(char escape)
    {
        assertInvalid("\\%s".formatted(escape), "Invalid escape sequence. Expected '{' after '\\%s'.".formatted(escape));
        assertInvalid("\\%s{Lt".formatted(escape), "Unclosed escape sequence. Expected '}' after 'Lt'.");
        assertInvalid("\\%s{lt}".formatted(escape), "Invalid character property: 'lt'");
        assertInvalid("\\%s{}".formatted(escape), "Invalid character property: ''");
        assertInvalid("\\%s{I}".formatted(escape), "Invalid character property: 'I'");
        assertInvalid("\\%s{Is}".formatted(escape), "Invalid character property: 'Is'");
        assertInvalid("\\%s{IzBasicLatin}".formatted(escape), "Invalid character property: 'IzBasicLatin'");
        assertInvalid("\\%s{isbasiclatin}".formatted(escape), "Invalid character property: 'isbasiclatin'");
        assertInvalid("\\%s{basiclatin}".formatted(escape), "Invalid character property: 'basiclatin'");
        assertInvalid("\\%s{ISBASICLATIN}".formatted(escape), "Invalid character property: 'ISBASICLATIN'");
        assertInvalid("\\%s{Isbasiclatin}".formatted(escape), "Invalid character property: 'Isbasiclatin'");
        assertInvalid("\\%s{IsBASICLATIN}".formatted(escape), "Invalid character property: 'IsBASICLATIN'");
        assertInvalid("\\%s{Kl}".formatted(escape), "Invalid character property: 'Kl'");
        assertInvalid("\\%s{Cs}".formatted(escape), "Invalid character property: 'Cs'");
        assertInvalid("\\%s{IsHighPrivateUseSurrogates}".formatted(escape), "Invalid character property: 'IsHighPrivateUseSurrogates'");
        assertInvalid("\\%s{IsLowSurrogates}".formatted(escape), "Invalid character property: 'IsLowSurrogates'");
        assertInvalid("\\%s{IsHighSurrogates}".formatted(escape), "Invalid character property: 'IsHighSurrogates'");
        assertInvalid("\\%s{NameChar}".formatted(escape), "Invalid character property: 'NameChar'");
        assertInvalid("\\%s{InitialNameChar}".formatted(escape), "Invalid character property: 'InitialNameChar'");
        assertInvalid("\\%s{IsNameChar}".formatted(escape), "Invalid character property: 'IsNameChar'");
        assertInvalid("\\%s{IsInitialNameChar}".formatted(escape), "Invalid character property: 'IsInitialNameChar'");
        assertInvalid("[\\%s{NameChar}]".formatted(escape), "Invalid character property: 'NameChar'");
        assertInvalid("[\\%s{InitialNameChar}]".formatted(escape), "Invalid character property: 'InitialNameChar'");
        assertInvalid("[\\%s{IsNameChar}]".formatted(escape), "Invalid character property: 'IsNameChar'");
        assertInvalid("[\\%s{IsInitialNameChar}]".formatted(escape), "Invalid character property: 'IsInitialNameChar'");
        assertInvalid("\\%s{IsLt}".formatted(escape), "Invalid character property: 'IsLt'");
        assertInvalid("\\%s{Lt&}".formatted(escape), "Invalid character property. Unexpected character '&'.");
        assertInvalid("[\\%s{IsLt}]".formatted(escape), "Invalid character property: 'IsLt'");
        assertInvalid("[\\%s{Lt&}]".formatted(escape), "Invalid character property. Unexpected character '&'.");
    }

    @Test
    void testGeneralCategoryEscape()
    {
        // According to the XSD 1.0 specification (https://www.w3.org/TR/xmlschema-2/#nt-IsCategory),
        // the general category 'Cs' is not supported because it "identifies 'surrogate' characters,
        // which do not occur at the level of the 'character abstraction' that XML instance documents
        // operate on."
        Set<String> categories = UNICODE_CHARACTERS.stream()
                .map(UnicodeCharacter::generalCategory)
                .filter(category -> !category.equals("Cs"))
                .collect(toImmutableSet());

        for (String category : categories) {
            for (UnicodeCharacter character : UNICODE_CHARACTERS) {
                if (character.generalCategory().equals("Cs")) {
                    continue;
                }

                String input = Character.toString(character.codePoint());
                if (character.generalCategory().equals(category)) {
                    assertMatches(input, "^\\p{%s}$".formatted(category));
                    assertMatches(input, "^[\\p{%s}]$".formatted(category));
                    assertDoesNotMatch(input, "^[^\\p{%s}]$".formatted(category));

                    assertDoesNotMatch(input, "^\\P{%s}$".formatted(category));
                    assertDoesNotMatch(input, "^[\\P{%s}]$".formatted(category));
                    assertMatches(input, "^[^\\P{%s}]$".formatted(category));
                }
                else {
                    assertDoesNotMatch(input, "^\\p{%s}$".formatted(category));
                    assertDoesNotMatch(input, "^[\\p{%s}]$".formatted(category));
                    assertMatches(input, "^[^\\p{%s}]$".formatted(category));

                    assertMatches(input, "^\\P{%s}$".formatted(category));
                    assertMatches(input, "^[\\P{%s}]$".formatted(category));
                    assertDoesNotMatch(input, "^[^\\P{%s}]$".formatted(category));
                }
            }
        }
    }

    @Test
    void testParentGeneralCategoryEscape()
    {
        // According to the XSD 1.0 specification (https://www.w3.org/TR/xmlschema-2/#nt-IsCategory),
        // the general category 'Cs' is not supported because it "identifies 'surrogate' characters,
        // which do not occur at the level of the 'character abstraction' that XML instance documents
        // operate on."
        Set<String> categories = UNICODE_CHARACTERS.stream()
                .filter(character -> !character.generalCategory().equals("Cs"))
                .map(UnicodeCharacter::parentGeneralCategory)
                .collect(toImmutableSet());

        for (String category : categories) {
            for (UnicodeCharacter character : UNICODE_CHARACTERS) {
                if (character.generalCategory().equals("Cs")) {
                    continue;
                }

                String input = Character.toString(character.codePoint());
                if (character.parentGeneralCategory().equals(category)) {
                    assertMatches(input, "^\\p{%s}$".formatted(category));
                    assertMatches(input, "^[\\p{%s}]$".formatted(category));
                    assertDoesNotMatch(input, "^[^\\p{%s}]$".formatted(category));

                    assertDoesNotMatch(input, "^\\P{%s}$".formatted(category));
                    assertDoesNotMatch(input, "^[\\P{%s}]$".formatted(category));
                    assertMatches(input, "^[^\\P{%s}]$".formatted(category));
                }
                else {
                    assertDoesNotMatch(input, "^\\p{%s}$".formatted(category));
                    assertDoesNotMatch(input, "^[\\p{%s}]$".formatted(category));
                    assertMatches(input, "^[^\\p{%s}]$".formatted(category));

                    assertMatches(input, "^\\P{%s}$".formatted(category));
                    assertMatches(input, "^[\\P{%s}]$".formatted(category));
                    assertDoesNotMatch(input, "^[^\\P{%s}]$".formatted(category));
                }
            }
        }
    }

    @Test
    void testBlockEscape()
    {
        for (UnicodeCharacter character : UNICODE_CHARACTERS) {
            // According to the XSD 1.0 specification (https://www.w3.org/TR/xmlschema-2/#nt-IsBlock):
            // - All whitespace must be stripped from block names.
            // - The HighSurrogates, LowSurrogates, and HighPrivateUseSurrogates blocks are not supported because these blocks
            //   "identify 'surrogate' characters, which do not occur at the level of the 'character abstraction' that XML
            //   instance documents operate on."
            String blockName = character.blockName().replaceAll(" ", "");
            if (blockName.equals("HighSurrogates") || blockName.equals("LowSurrogates") || blockName.equals("HighPrivateUseSurrogates")) {
                continue;
            }

            String input = Character.toString(character.codePoint());

            assertMatches(input, "^\\p{Is%s}$".formatted(blockName));
            assertMatches(input, "^[\\p{Is%s}]$".formatted(blockName));
            assertDoesNotMatch(input, "^[^\\p{Is%s}]$".formatted(blockName));

            assertDoesNotMatch(input, "^\\P{Is%s}$".formatted(blockName));
            assertDoesNotMatch(input, "^[\\P{Is%s}]$".formatted(blockName));
            assertMatches(input, "^[^\\P{Is%s}]$".formatted(blockName));
        }
    }

    @Test
    void testMultiCharacterEscapeForEmptyString()
    {
        testMultiCharacterEscapeForEmptyString('c');
        testMultiCharacterEscapeForEmptyString('C');
        testMultiCharacterEscapeForEmptyString('w');
        testMultiCharacterEscapeForEmptyString('W');
        testMultiCharacterEscapeForEmptyString('d');
        testMultiCharacterEscapeForEmptyString('D');
        testMultiCharacterEscapeForEmptyString('i');
        testMultiCharacterEscapeForEmptyString('I');
        testMultiCharacterEscapeForEmptyString('s');
        testMultiCharacterEscapeForEmptyString('S');
    }

    private void testMultiCharacterEscapeForEmptyString(char escape)
    {
        assertDoesNotMatch("ab", "a\\%sb".formatted(escape));
        assertDoesNotMatch("ab", "a[\\%s]b".formatted(escape));
        assertDoesNotMatch("ab", "a[^\\%s]b".formatted(escape));
    }

    @Test
    void testDecimalDigitEscape()
    {
        Set<Integer> decimalDigitCodePoints = UNICODE_CHARACTERS.stream()
                .filter(character -> character.generalCategory().equals("Nd"))
                .map(UnicodeCharacter::codePoint)
                .collect(toImmutableSet());

        for (int codePoint = MIN_CODE_POINT; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }

            String input = Character.toString(codePoint);
            if (decimalDigitCodePoints.contains(codePoint)) {
                assertMatches(input, "^\\d$");
                assertMatches(input, "^[\\d]$");
                assertDoesNotMatch(input, "^[^\\d]$");

                assertDoesNotMatch(input, "^\\D$");
                assertDoesNotMatch(input, "^[\\D]$");
                assertMatches(input, "^[^\\D]$");
            }
            else {
                assertDoesNotMatch(input, "^\\d$");
                assertDoesNotMatch(input, "^[\\d]$");
                assertMatches(input, "^[^\\d]$");

                assertMatches(input, "^\\D$");
                assertMatches(input, "^[\\D]$");
                assertDoesNotMatch(input, "^[^\\D]$");
            }
        }
    }

    @Test
    void testWEscape()
    {
        Set<Integer> punctuationSeparatorAndOtherCodePoints = UNICODE_CHARACTERS.stream()
                .filter(character -> character.isPunctuation() || character.isSeparator() || character.isOther())
                .map(UnicodeCharacter::codePoint)
                .collect(toImmutableSet());

        for (int codePoint = MIN_CODE_POINT; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }

            String input = Character.toString(codePoint);
            if (punctuationSeparatorAndOtherCodePoints.contains(codePoint)) {
                assertDoesNotMatch(input, "^\\w$");
                assertDoesNotMatch(input, "^[\\w]$");
                assertMatches(input, "^[^\\w]$");

                assertMatches(input, "^\\W$");
                assertMatches(input, "^[\\W]$");
                assertDoesNotMatch(input, "^[^\\W]$");
            }
            else {
                assertMatches(input, "^\\w$");
                assertMatches(input, "^[\\w]$");
                assertDoesNotMatch(input, "^[^\\w]$");

                assertDoesNotMatch(input, "^\\W$");
                assertDoesNotMatch(input, "^[\\W]$");
                assertMatches(input, "^[^\\W]$");
            }
        }
    }

    @Test
    void testInitialNameCharacterEscape()
    {
        Set<Integer> initialNameCharacterCodePoints = loadCodePoints("InitialNameCharRanges.csv");
        for (int codePoint = MIN_CODE_POINT; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }

            String input = Character.toString(codePoint);
            if (initialNameCharacterCodePoints.contains(codePoint)) {
                assertMatches(input, "^\\i$");
                assertMatches(input, "^[\\i]$");
                assertDoesNotMatch(input, "^[^\\i]$");

                assertDoesNotMatch(input, "^\\I$");
                assertDoesNotMatch(input, "^[\\I]$");
                assertMatches(input, "^[^\\I]$");
            }
            else {
                assertDoesNotMatch(input, "^\\i$");
                assertDoesNotMatch(input, "^[\\i]$");
                assertMatches(input, "^[^\\i]$");

                assertMatches(input, "^\\I$");
                assertMatches(input, "^[\\I]$");
                assertDoesNotMatch(input, "^[^\\I]$");
            }
        }
    }

    @Test
    void testNameCharacterEscape()
    {
        Set<Integer> nameCharacterCodePoints = loadCodePoints("NameCharRanges.csv");
        for (int codePoint = MIN_CODE_POINT; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }

            String input = Character.toString(codePoint);
            if (nameCharacterCodePoints.contains(codePoint)) {
                assertMatches(input, "^\\c$");
                assertMatches(input, "^[\\c]$");
                assertDoesNotMatch(input, "^[^\\c]$");

                assertDoesNotMatch(input, "^\\C$");
                assertDoesNotMatch(input, "^[\\C]$");
                assertMatches(input, "^[^\\C]$");
            }
            else {
                assertDoesNotMatch(input, "^\\c$");
                assertDoesNotMatch(input, "^[\\c]$");
                assertMatches(input, "^[^\\c]$");

                assertMatches(input, "^\\C$");
                assertMatches(input, "^[\\C]$");
                assertDoesNotMatch(input, "^[^\\C]$");
            }
        }
    }

    @Test
    void testWhitespaceEscape()
    {
        Set<Integer> singleCharacterWhitespaceCodePoints = ImmutableSet.of(' ', '\t', '\n', '\u000B', '\u000C', '\r', '\u0085', '\u2028', '\u2029').stream()
                .map(character -> (int) character)
                .collect(toImmutableSet());

        for (int codePoint = MIN_CODE_POINT; codePoint <= MAX_CODE_POINT; codePoint++) {
            if (MIN_SURROGATE <= codePoint && codePoint <= MAX_SURROGATE) {
                continue;
            }

            String input = Character.toString(codePoint);
            if (singleCharacterWhitespaceCodePoints.contains(codePoint)) {
                assertMatches(input, "^\\s$");
                assertMatches(input, "^[\\s]$");
                assertDoesNotMatch(input, "^[^\\s]$");

                assertDoesNotMatch(input, "^\\S$");
                assertDoesNotMatch(input, "^[\\S]$");
                assertMatches(input, "^[^\\S]$");
            }
            else {
                assertDoesNotMatch(input, "^\\s$");
                assertDoesNotMatch(input, "^[\\s]$");
                assertMatches(input, "^[^\\s]$");

                assertMatches(input, "^\\S$");
                assertMatches(input, "^[\\S]$");
                assertDoesNotMatch(input, "^[^\\S]$");
            }
        }
    }

    @Test
    void testWhitespaceEscapeForCrlf()
    {
        assertMatches("\r\n", "^\\s$");
        assertMatches("\r\n", "^[\\s]$");
        assertDoesNotMatch("\r\n", "^[^\\s]$");
        assertDoesNotMatch("\r\n", "^[^\\s][^\\s]$");

        // the first \s matches \r\n, the second \s matches either \n or \r
        assertMatches("\r\n\n", "^\\s\\s$");
        assertMatches("\r\n\r", "^\\s\\s$");
        assertMatches("\r\n\n", "^[\\s][\\s]$");
        assertMatches("\r\n\r", "^[\\s][\\s]$");

        // the first \s matches \r\n, the second \s doesn't match anything
        assertDoesNotMatch("\r\n", "^\\s\\s$");
        assertDoesNotMatch("\r\n", "^[\\s][\\s]$");

        // \r matches \r, \s matches \n
        assertMatches("\r\n", "^\\r\\s$");
        assertMatches("\r\n", "^[\\r][\\s]$");

        // \s matches \r\n, \n doesn't match anything
        assertDoesNotMatch("\r\n", "^\\s\\n$");
        assertDoesNotMatch("\r\n", "^[\\s][\\n]$");

        assertDoesNotMatch("\r\n", "^\\S$");
        assertDoesNotMatch("\r\n", "^\\S\\S$");
        assertDoesNotMatch("\r\n", "^[\\S]$");
        assertDoesNotMatch("\r\n", "^[^\\S]$");
        assertMatches("\r\n", "^[^\\S][^\\S]$");
    }

    @Test
    void testInvalidCharacterClassExpression()
    {
        assertInvalid("[]", "Character group must not be empty");
        assertInvalid("[]]", "Character group must not be empty");
        assertInvalid("[^]", "Character group must not be empty");
        assertInvalid("[[]", "Unescaped '[' within character class expression");
        assertInvalid("[z-a]", "Invalid character range: start > end");
        assertInvalid("[a-\\n]", "Invalid character range: start > end");
        assertInvalid("[a--b]", "Unescaped '-' within character class expression");
        assertInvalid("[---]", "Unescaped '-' cannot act as end of range");
        assertInvalid("[\\n--]", "Unescaped '-' cannot act as end of range");
        assertInvalid("[--a]", "Unescaped '-' cannot act as start of range");
        assertInvalid("[a--]", "Unescaped '-' cannot act as end of range");
        assertInvalid("[a-z", "Unclosed character class expression");
        assertInvalid("[a-\\c", "Multi-character escape cannot follow '-'");
        assertInvalid("[\\c-\\s]", "Multi-character escape cannot follow '-'");
        assertInvalid("[\\c-a]", "Unescaped '-' within character class expression");
        assertInvalid("[a-k-z]", "Unescaped '-' within character class expression");
        assertInvalid("[[]]", "Unescaped '[' within character class expression");
        assertInvalid("[[]]", "Unescaped '[' within character class expression");
    }

    @Test
    void testCharacterClassExpression()
    {
        assertMatches("a", "[ab]");
        assertMatches("b", "[ab]");
        assertDoesNotMatch("c", "[ab]");
        assertMatches("\uD83C\uDC00", "[\uD83C\uDC00£ਞ]");
        assertMatches("£", "[\uD83C\uDC00£ਞ]");
        assertMatches("ਞ", "[\uD83C\uDC00£ਞ]");

        assertMatches("3", "[0-9a-z]");
        assertMatches("g", "[0-9a-z]");
        assertDoesNotMatch("%", "[0-9a-z]");
        assertDoesNotMatch("G", "[0-9a-z]");

        assertDoesNotMatch("a", "[^ab]");
        assertDoesNotMatch("b", "[^ab]");
        assertMatches("c", "[^ab]");

        assertDoesNotMatch("3", "[^0-9a-z]");
        assertDoesNotMatch("g", "[^0-9a-z]");
        assertMatches("%", "[^0-9a-z]");
        assertMatches("G", "[^0-9a-z]");

        assertMatches("-", "[-a-]");
        assertMatches("a", "[-a-]");
        assertMatches("-", "[-]");
        assertMatches("-", "[a-]");
        assertMatches("-", "[-a]");
        assertMatches("-", "[\\--\\-]");
        assertMatches("-", "[--]");
        assertDoesNotMatch("^", "[^^]");
        assertMatches("-", "[^^]");
        assertMatches("(", "[()]");
        assertMatches(")", "[()]");

        // Some regex syntaxes interpret '&&' within [...] as a character class intersection. In XQuery, it has no special meaning.
        assertMatches("&", "[a-z&&]");
        assertMatches("a", "[a-z&&]");
        assertMatches("&", "[&]");
    }

    @Test
    void testCharacterClassSubtraction()
    {
        assertMatches("a", "[a-z-[c]]");
        assertDoesNotMatch("c", "[a-z-[c]]");
        assertDoesNotMatch("a", "[a-[a]]");
        assertMatches("a", "[a-[a-[a]]]");
        assertMatches("-", "[a-z--[a-b]]");
        assertDoesNotMatch("a", "[a-z--[a-b]]");
        assertDoesNotMatch("-", "[--[-]]");
        assertDoesNotMatch("a", "[--[-]]");
        assertMatches("a", "[a-[z]]");

        assertDoesNotMatch("a", "[^a-[a]]");
        assertDoesNotMatch("a", "[^a-[^a]]");
        assertMatches("a", "[a-[^a]]");
        assertDoesNotMatch("a", "[^a-c-[0-5]]");
        assertDoesNotMatch("5", "[^a-c-[0-5]]");
        assertMatches("d", "[^a-c-[0-5]]");
        assertMatches("9", "[^a-c-[0-5]]");

        assertMatches("\n", "^[\\s-[\\s-[\n]]]$");
        assertMatches("\n", "^[\\s-[\\s-[\\n]]]$");
        assertDoesNotMatch("\r", "^[\\s-[\\s-[\n]]]$");
        assertDoesNotMatch("\r", "^[\\s-[\\s-[\\n]]]$");

        // This subtracts two characters: \n and \r. The CRLF sequence is still present.
        assertMatches("\r\n", "^[\\s-[\n\r]]$");
        assertMatches("\r\n", "^[\\s-[\r\n]]$");
        assertMatches("\r\n", "^[\\s-[\\n\\r]]$");
        assertMatches("\r\n", "^[\\s-[\\r\\n]]$");
        assertDoesNotMatch("\n", "^[\\s-[\n\r]]$");
        assertDoesNotMatch("\n", "^[\\s-[\r\n]]$");
        assertDoesNotMatch("\n", "^[\\s-[\\n\\r]]$");
        assertDoesNotMatch("\n", "^[\\s-[\\r\\n]]$");
        assertDoesNotMatch("\r", "^[\\s-[\n\r]]$");
        assertDoesNotMatch("\r", "^[\\s-[\r\n]]$");
        assertDoesNotMatch("\r", "^[\\s-[\\n\\r]]$");
        assertDoesNotMatch("\r", "^[\\s-[\\r\\n]]$");

        assertDoesNotMatch("\r\n", "^[\\s-[\\s-[\\n\\r]]]$");
        assertMatches("\r", "^[\\s-[\\s-[\\n\\r]]]$");
        assertMatches("\n", "^[\\s-[\\s-[\\n\\r]]]$");

        assertMatches("\r\n", "^[\\s-[^\\s]]$");
        assertMatches("\r", "^[\\s-[^\\s]]$");
        assertMatches("\n", "^[\\s-[^\\s]]$");
    }

    @Test
    void testInvalidCharacterClassSubtraction()
    {
        assertInvalid("[-[a-b]]", "Character group must not be empty");
        assertInvalid("[a-z---[a-b]]", "Unescaped '-' cannot act as end of range");
        assertInvalid("[a-z-[a-b]0-9]", "Expected ']' after subtraction");
        assertInvalid("[a-z[z-a]]", "Unescaped '[' within character class expression");
        assertInvalid("[a-z-[z-a]]", "Invalid character range: start > end");
        assertInvalid("[a-z-[]]", "Character group must not be empty");
        assertInvalid("[a-z-[^aeiou]", "Expected ']' after subtraction");
        // Make sure that '&&[...]' within [...] is illegal (some regex syntaxes interpret it as a character class intersection)
        assertInvalid("[a-z&&[^aeiou]]", "Unescaped '[' within character class expression");
    }

    @Test
    void testSkipWhitespaceMode()
    {
        assertMatches("helloworld", "hello\n \r\tworld", "x");
        assertDoesNotMatch("hello world", "hello \r\n\tworld", "x");
        assertMatches("hello world", "hello[\r\n\t ]world ", "x");
        assertMatches("hello\rworld", "hello[\r\n\t ]world ", "x");
        assertMatches("hello\nworld", "hello[\r\n\t ]world ", "x");
        assertMatches("hello\tworld", "hello[\r\n\t ]world ", "x");
        assertDoesNotMatch("helloworld", "hello[ \r\n\t]world", "x");

        assertInvalid("[\\p{ I s B a s i c L a t i n }]+", "x", "Invalid character property. Unexpected character ' '.");
        assertInvalid("[\\p{\nI\ns\nB\na\ns\ni\nc\nL\na\nt\ni\nn\n}]+", "x", "Invalid character property. Unexpected character '\n'.");
        assertInvalid("[\\p{\rI\rs\rB\ra\rs\ri\rc\rL\ra\rt\ri\rn\r}]+", "x", "Invalid character property. Unexpected character '\r'.");
        assertInvalid("[\\p{\tI\ts\tB\ta\ts\ti\tc\tL\ta\tt\ti\tn\t}]+", "x", "Invalid character property. Unexpected character '\t'.");
        assertInvalid("[\\ s]+", "x", "Unexpected escape sequence: '\\ '");
        assertInvalid("[\\\ns]+", "x", "Unexpected escape sequence: '\\\n'");
        assertInvalid("[\\\rs]+", "x", "Unexpected escape sequence: '\\\r'");
        assertInvalid("[\\\ts]+", "x", "Unexpected escape sequence: '\\\t'");
        assertMatches("hello world", "hello\\ \t\r\nsworld", "x");
        assertMatches("hello world", "\\p{ I s B\na s\ri c L a t\ti n }+", "x");

        assertMatches("aa", "(a)\\\r\n\t 1", "x");
        assertMatches("abcdefghia0jj", "(a)(b)(c)(d)(e)(f)(g)(h)(i)\\\r\t\n 10 (j)\\ \n1\r 0\t", "x");

        assertMatches("aaa", "a {\n1, \t3\r}", "x");
    }

    @Test
    void testWildcardEscape()
    {
        assertMatches("a", "^.$");
        assertMatches("a", "^.$", "s");

        // empty string
        assertDoesNotMatch("", "^.$");
        assertDoesNotMatch("", "^.$", "s");

        // multibyte character
        assertMatches("\uD83C\uDC00", "^.$");
        assertMatches("\uD83C\uDC00", "^.$", "s");

        // '.' has no special meaning within [...]
        assertMatches(".", "[.]");
        assertMatches(".", "[.]", "s");
        assertDoesNotMatch("a", "[.]");
        assertDoesNotMatch("a", "[.]", "s");

        assertMatches("\r\n\r", "^..$", "s");
        assertMatches("\r\n\n", "^..$", "s");
        assertMatches("\n\r\n", "^..$", "s");
        assertMatches("\r\r\n", "^..$", "s");
        for (String separator : LINE_SEPARATORS) {
            assertDoesNotMatch(separator, "^.$");
            assertDoesNotMatch(separator, "^.$", "m");
            assertMatches(separator, "^.$", "s");
            assertMatches(separator, "^$.", "s");
            assertDoesNotMatch(separator, "^.$.", "s");
            assertMatches(separator.repeat(2), "^.$.", "s");
            assertDoesNotMatch(separator, "^..$", "s");
            assertDoesNotMatch(separator.repeat(2), "^$..", "s");
            assertDoesNotMatch(separator.repeat(2), "..^$", "s");
            assertMatches(separator.repeat(2), "^$..", "sm");
            assertMatches(separator.repeat(2), "..^$", "sm");
        }
    }

    @Test
    void testStartMetaCharacterForSingleLine()
    {
        testStartMetaCharacterForSingleLine("");
        testStartMetaCharacterForSingleLine("m");
    }

    private void testStartMetaCharacterForSingleLine(String flags)
    {
        assertMatches("", "^", flags);
        assertMatches("abc", "^", flags);
        assertMatches("abc", "^abc", flags);
        assertDoesNotMatch("aabc", "^abc", flags);
        assertMatches("abcc", "^abc", flags);
        assertMatches("a", "^^", flags);
        assertMatches("", "^^", flags);
        assertMatches("", "^^^", flags);
        assertDoesNotMatch("a", "^\\^", flags);
        assertDoesNotMatch("", "^\\^", flags);
        assertMatches("^", "^\\^", flags);
        assertDoesNotMatch("a", "^a^", flags);
        assertDoesNotMatch("a^", "^a^", flags);
        assertMatches("a^", "^a\\^", flags);
        assertDoesNotMatch("ab", "ab^", flags);
    }

    @Test
    void testStartMetaCharacterForMultipleLines()
    {
        for (String separator : LINE_SEPARATORS) {
            // default mode
            assertMatches(separator, "^");
            assertMatches(separator + "abc", "^");
            assertMatches(separator + "abc", "^" + separator + "abc");
            assertDoesNotMatch(separator + "abc", "^abc");
            assertDoesNotMatch(separator + "abc", separator + "^abc");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc" + separator + "ddd", "^ccc");
            assertDoesNotMatch(separator.repeat(2) + "abc", "^abc");
            assertDoesNotMatch("\n" + separator + "abc", "^abc");
            assertDoesNotMatch("\r" + separator + "abc", "^abc");

            // multi-line mode
            assertMatches(separator, "^", "m");
            assertMatches(separator + "abc", "^", "m");
            assertMatches(separator + "abc", "^abc", "m");
            assertMatches(separator + "abc", separator + "^abc", "m");
            assertMatches("aaa" + separator + "bbb" + separator + "ccc" + separator + "ddd", "^ccc", "m");
            assertMatches(separator.repeat(2) + "abc", "^abc", "m");
            assertMatches("\n" + separator + "abc", "^abc", "m");
            assertMatches("\r" + separator + "abc", "^abc", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc", "^abbb", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc", "^bbb^ccc", "m");
            assertMatches("aaa" + separator + "bbb" + separator + "ccc", "^bbb" + separator + "^ccc", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc", "^bbb" + separator.repeat(2) + "^ccc", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc", "^bbb" + separator + "\r^ccc", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "ccc", "^bbb" + separator + "\n^ccc", "m");
        }
    }

    @Test
    void testEndMetaCharacterForSingleLine()
    {
        testEndMetaCharacterForSingleLine("");
        testEndMetaCharacterForSingleLine("m");
    }

    private void testEndMetaCharacterForSingleLine(String flags)
    {
        assertMatches("", "$", flags);
        assertMatches("", "$$", flags);
        assertMatches("abc", "$", flags);
        assertMatches("abc", "$$", flags);
        assertMatches("abc", "abc$", flags);
        assertMatches("abc", "abc$$", flags);
        assertMatches("aabc", "abc$", flags);
        assertMatches("aabc", "abc$$", flags);
        assertDoesNotMatch("abcc", "abc$", flags);

        assertMatches("abc\r\n", "abc$\r", flags);
        assertMatches("abc\r\n", "abc$$\r", flags);
        assertDoesNotMatch("abc\r", "abc$\r\n", flags);
        assertDoesNotMatch("abc\r\n", "abc\r$", flags);
        assertDoesNotMatch("abc\r\n", "abc\n$", flags);
        assertDoesNotMatch("abc\r\n", "abc$\n", flags);
        assertDoesNotMatch("abc\r\n", "abc\r$\n", flags);

        assertDoesNotMatch("abc ", "abc$", flags);
        assertDoesNotMatch("abc\t", "abc$", flags);
        assertDoesNotMatch("abc ", "abc$ ", flags);
        assertDoesNotMatch("abc\t", "abc$\t", flags);
        assertMatches("abc ", "abc $", flags);
        assertMatches("abc\t", "abc\t$", flags);
    }

    @Test
    void testEndMetaCharacterForStringEndedWithNewline()
    {
        for (String separator : LINE_SEPARATORS) {
            // default mode
            assertMatches("abc" + separator, "$");
            assertMatches("abc" + separator, "abc$");
            assertMatches("abc" + separator, "abc$" + separator);
            assertMatches("abc" + separator, "abc" + separator + "$");
            assertDoesNotMatch("abc" + separator, "abc" + separator + "$" + separator);
            assertDoesNotMatch("abc" + separator.repeat(2), "abc$");
            assertMatches("abc" + separator.repeat(2), "abc" + separator + "$" + separator);
            assertDoesNotMatch("abc" + separator.repeat(2), "abc$" + separator.repeat(2));
            assertDoesNotMatch("abc" + separator.repeat(2), "abc$$");
            assertMatches("abc" + separator.repeat(2), "abc" + separator + "$");
            assertDoesNotMatch("abc" + separator.repeat(2), "abc" + separator.repeat(3) + "$");

            // multi-line mode
            assertMatches("abc" + separator, "$", "m");
            assertMatches("abc" + separator, "abc$", "m");
            assertMatches("abc" + separator, "abc$" + separator, "m");
            assertMatches("abc" + separator, "abc" + separator + "$", "m");
            assertDoesNotMatch("abc" + separator, "abc" + separator + "$" + separator, "m");
            assertMatches("abc" + separator.repeat(2), "abc$", "m");
            assertMatches("abc" + separator.repeat(2), "abc" + separator + "$" + separator, "m");
            assertMatches("abc" + separator.repeat(2), "abc$" + separator.repeat(2), "m");
            assertMatches("abc" + separator.repeat(2), "abc$$", "m");
            assertMatches("abc" + separator.repeat(2), "abc" + separator + "$", "m");
            assertDoesNotMatch("abc" + separator.repeat(2), "abc" + separator.repeat(3) + "$", "m");
        }
    }

    @Test
    void testEndMetaCharacterForMultipleLines()
    {
        for (String separator : LINE_SEPARATORS) {
            // default mode
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "cabc" + separator + "ccc", "abc$");
            assertMatches("aaa" + separator + "bbb" + separator + "cabc", "abc$");

            // multi-line mode
            assertMatches("aaa" + separator + "bbb" + separator + "cabc" + separator + "ccc", "abc$", "m");
            assertMatches("aaa" + separator + "bbb" + separator + "cabc", "abc$", "m");
            assertDoesNotMatch("aaa" + separator + "bbb" + separator + "abcb" + separator + "ccc", "abc$", "m");
        }
    }

    @Test
    void testEmptyLine()
    {
        for (String separator : LINE_SEPARATORS) {
            assertMatches("a" + separator.repeat(2) + "b", "^$", "m");
            assertDoesNotMatch("a" + separator + "b", "^$", "m");
        }
    }

    @Test
    void testNoEmptyLineWithinCrlf()
    {
        assertDoesNotMatch("a\r\nb", "^$", "m");
        assertMatches("a\n\rb", "^$", "m");
    }

    @Test
    @Timeout(2)
    void testInvalidUtf8()
    {
        assertInvalid(".*", "\uD83C \uDC00", "The provided flag parameter is not a valid UTF-8 string");
        assertInvalid(".*", "\uD83C", "The provided flag parameter is not a valid UTF-8 string");
        assertInvalid(".*", "\uDC00", "The provided flag parameter is not a valid UTF-8 string");

        assertInvalid("(\uD83Ci)ABC", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("(\uD83Cs)ABC", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("((\uD83C:abc)\uDC00(£ਞ))", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("\uD83C \uDC00", "x", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("[\uD83C \uDC00]", "x", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("\uD83C \uDC00", "The provided pattern is not a valid UTF-8 string");
        assertInvalid("\uD83C", "The provided pattern is not a valid UTF-8 string");

        // Tests that XQuerySqlRegex.match doesn't loop infinitely on invalid UTF-8 input. Return value is irrelevant.
        Slice invalidUtf8Bytes = Slices.wrappedBuffer(new byte[] {
                // AAA\uD800AAAA\uDFFFAAA, U+D800 and U+DFFF are valid Unicode code points but are not valid in UTF-8.
                (byte) 0x41, 0x41, (byte) 0xed, (byte) 0xa0, (byte) 0x80, 0x41, 0x41,
                0x41, 0x41, (byte) 0xed, (byte) 0xbf, (byte) 0xbf, 0x41, 0x41, 0x41
        });
        match(invalidUtf8Bytes, ".*", "");
    }

    private static void assertMatches(String input, String pattern, String flags)
    {
        assertThat(match(utf8Slice(input), pattern, flags))
                .withFailMessage("Pattern: '%s', flags: '%s', input: '%s'", escape(pattern), escape(flags), escape(input))
                .isTrue();
    }

    private static void assertMatches(String input, String pattern)
    {
        assertMatches(input, pattern, "");
    }

    private static void assertDoesNotMatch(String input, String pattern, String flags)
    {
        assertThat(match(utf8Slice(input), pattern, flags))
                .withFailMessage("Pattern: '%s', flags: '%s', input: '%s'", escape(pattern), escape(flags), escape(input))
                .isFalse();
    }

    private static void assertDoesNotMatch(String input, String pattern)
    {
        assertDoesNotMatch(input, pattern, "");
    }

    private static void assertInvalid(String pattern, String errorMessage)
    {
        assertInvalid(pattern, "", errorMessage);
    }

    private static void assertInvalid(String pattern, String flags, String errorMessage)
    {
        assertThatThrownBy(() -> match(EMPTY_SLICE, pattern, flags))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage(errorMessage);
    }

    private static String escape(String input)
    {
        return input.replaceAll("\t", "\\\\t")
                .replaceAll("\r", "\\\\r")
                .replaceAll("\n", "\\\\n")
                .replaceAll("\u000B", "\\\\u000B")
                .replaceAll("\u000C", "\\\\u000C")
                .replaceAll("\u0085", "\\\\u0085")
                .replaceAll("\u2028", "\\\\u2028")
                .replaceAll("\u2029", "\\\\u2029");
    }

    private static boolean match(Slice input, String pattern, String flags)
    {
        XQuerySqlRegex regex = XQuerySqlRegex.compile(pattern, Optional.of(flags));
        return regex.match(input);
    }

    private static Set<Integer> loadCodePoints(String filename)
    {
        String path = "io/trino/json/regex/" + filename;
        URL rangesUrl = TestXQuerySqlRegex.class.getClassLoader().getResource(path);
        verify(rangesUrl != null, "%s not found", path);
        try {
            return Files.readAllLines(Path.of(rangesUrl.getPath()))
                    .stream()
                    .flatMapToInt(line -> {
                        String[] range = line.split(",");
                        int from = Integer.parseInt(range[0].substring(2), 16);
                        int to = Integer.parseInt(range[1].substring(2), 16);
                        return IntStream.rangeClosed(from, to);
                    })
                    .boxed()
                    .collect(toImmutableSet());
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static List<UnicodeCharacter> loadUnicodeCharacters()
    {
        try {
            URL blocksUrl = TestXQuerySqlRegex.class.getClassLoader().getResource("io/trino/json/regex/Blocks-4.txt");
            verify(blocksUrl != null, "io/trino/json/regex/Blocks-4.txt not found");
            RangeMap<Integer, String> blocks = TreeRangeMap.create();
            for (String line : Files.readAllLines(Path.of(blocksUrl.getPath()))) {
                if (!line.startsWith("#") && !line.isEmpty()) {
                    String[] columns = line.split(";");
                    String[] range = columns[0].split("\\.\\.");
                    int from = Integer.parseInt(range[0], 16);
                    int to = Integer.parseInt(range[1], 16);
                    String blockName = columns[1].trim();
                    blocks.put(Range.closed(from, to), blockName);
                }
            }

            URL unicodeDataUrl = TestXQuerySqlRegex.class.getClassLoader().getResource("io/trino/json/regex/UnicodeData-3.1.0.txt");
            verify(unicodeDataUrl != null, "io/trino/json/regex/UnicodeData-3.1.0.txt not found");
            return Files.readAllLines(Path.of(unicodeDataUrl.getPath()))
                    .stream()
                    .map(line -> {
                        String[] columns = line.split(";");
                        int codePoint = Integer.parseInt(columns[0], 16);
                        String generalCategoryName = columns[2];
                        String blockName = blocks.get(codePoint);
                        verify(blockName != null, "block name for %s not found", columns[0]);
                        return new UnicodeCharacter(codePoint, generalCategoryName, blockName);
                    })
                    .toList();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private record UnicodeCharacter(int codePoint, String generalCategory, String blockName)
    {
        public UnicodeCharacter
        {
            checkArgument(codePoint >= MIN_CODE_POINT && codePoint <= MAX_CODE_POINT, "codePoint must be between %s and %s", MIN_CODE_POINT, MAX_CODE_POINT);
            requireNonNull(generalCategory, "generalCategory is null");
            requireNonNull(blockName, "blockName is null");
        }

        public boolean isPunctuation()
        {
            return generalCategory.startsWith("P");
        }

        public boolean isSeparator()
        {
            return generalCategory.startsWith("Z");
        }

        public boolean isOther()
        {
            return generalCategory.startsWith("C");
        }

        public String parentGeneralCategory()
        {
            return String.valueOf(generalCategory.charAt(0));
        }
    }
}
