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
package io.trino.operator.scalar;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import io.trino.type.LikePattern;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.util.Optional;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.type.LikeFunctions.isLikePattern;
import static io.trino.type.LikeFunctions.likeChar;
import static io.trino.type.LikeFunctions.likePattern;
import static io.trino.type.LikeFunctions.likeVarchar;
import static io.trino.type.LikeFunctions.patternConstantPrefixBytes;
import static io.trino.type.LikeFunctions.unescapeLiteralLikePattern;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestLikeFunctions
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    private static Slice offsetHeapSlice(String value)
    {
        Slice source = Slices.utf8Slice(value);
        Slice result = Slices.allocate(source.length() + 5);
        result.setBytes(2, source);
        return result.slice(2, source.length());
    }

    @Test
    public void testLikeBasic()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("f%b__").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("foobar"), matcher)).isTrue();
        assertThat(likeVarchar(offsetHeapSlice("foobar"), matcher)).isTrue();

        assertThat(assertions.expression("a LIKE 'f%b__'")
                .binding("a", "'foob'"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'f%b'")
                .binding("a", "'foob'"))
                .isEqualTo(true);

        // value with explicit type (formal type potentially longer than actual length)
        assertThat(assertions.expression("a LIKE 'foo '")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foo '")
                .binding("a", "CAST('foo ' AS varchar(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo___'")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foo%'")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(true);

        // value and pattern with explicit type (formal type potentially longer than actual length)
        assertThat(assertions.expression("a LIKE CAST('foo' AS varchar(6))")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE CAST('foo  ' AS varchar(3))")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(true); // pattern gets truncated
        assertThat(assertions.expression("a LIKE CAST('foo   ' AS varchar(6))")
                .binding("a", "CAST('foo' AS varchar(6))"))
                .isEqualTo(false);
    }

    @Test
    public void testLikeChar()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("f%b__").toStringUtf8(), Optional.empty());
        assertThat(likeChar(6L, utf8Slice("foobar"), matcher)).isTrue();
        assertThat(likeChar(6L, offsetHeapSlice("foobar"), matcher)).isTrue();
        assertThat(likeChar(6L, utf8Slice("foob"), matcher)).isTrue();
        assertThat(likeChar(6L, offsetHeapSlice("foob"), matcher)).isTrue();
        assertThat(likeChar(7L, utf8Slice("foob"), matcher)).isFalse();
        assertThat(likeChar(7L, offsetHeapSlice("foob"), matcher)).isFalse();

        // pattern shorter than value length
        assertThat(assertions.expression("a LIKE 'foo'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foo  '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'fo_'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'fo%'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE '%foo'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE '_oo'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'f%b__'")
                .binding("a", "CAST('foob' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'f%b__'")
                .binding("a", "CAST('foob' AS char(7))"))
                .isEqualTo(false);

        // pattern of length equal to value length
        assertThat(assertions.expression("a LIKE 'foo'")
                .binding("a", "CAST('foo' AS char(3))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'jaźń'")
                .binding("a", "CAST('jaźń' AS char(4))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'fob'")
                .binding("a", "CAST('foo' AS char(3))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foo   '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo __'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE '%%%%%%'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);

        // pattern longer than value length
        assertThat(assertions.expression("a LIKE '%%foo'")
                .binding("a", "CAST('foo' AS char(3))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'f#_#_' ESCAPE '#'")
                .binding("a", "CAST('foo' AS char(3))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'f#_#_' ESCAPE '#'")
                .binding("a", "CAST('f__' AS char(3))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo    '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foo __ '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE '_______'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE '%%%%%%%'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo   %%%%%%%'")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo  %%%%%%% '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(true);
        assertThat(assertions.expression("a LIKE 'foo  %%%%%%%  '")
                .binding("a", "CAST('foo' AS char(6))"))
                .isEqualTo(false);
        assertThat(assertions.expression("a LIKE 'foobar%%%%%%%'")
                .binding("a", "CAST('foobar' AS char(6))"))
                .isEqualTo(true);
    }

    @Test
    public void testLikeSpacesInPattern()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("ala  ").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("ala  "), matcher)).isTrue();
        assertThat(likeVarchar(utf8Slice("ala"), matcher)).isFalse();
    }

    @Test
    public void testLikeNewlineInPattern()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("%o\nbar").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("foo\nbar"), matcher)).isTrue();
    }

    @Test
    public void testLikeNewlineBeforeMatch()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("%b%").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("foo\nbar"), matcher)).isTrue();
    }

    @Test
    public void testLikeNewlineInMatch()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("f%b%").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("foo\nbar"), matcher)).isTrue();
    }

    @Test
    public void testLikeUtf8Pattern()
    {
        LikePattern matcher = likePattern(utf8Slice("%\u540d\u8a89%"), utf8Slice("\\"));
        assertThat(likeVarchar(utf8Slice("foo"), matcher)).isFalse();
    }

    @Test
    public void testLikeInvalidUtf8Value()
    {
        Slice value = Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'});
        LikePattern matcher = likePattern(utf8Slice("%b%"), utf8Slice("\\"));
        assertThat(likeVarchar(value, matcher)).isTrue();
    }

    @Test
    public void testBackslashesNoSpecialTreatment()
    {
        LikePattern matcher = LikePattern.compile(utf8Slice("\\abc\\/\\\\").toStringUtf8(), Optional.empty());
        assertThat(likeVarchar(utf8Slice("\\abc\\/\\\\"), matcher)).isTrue();
    }

    @Test
    public void testSelfEscaping()
    {
        LikePattern matcher = likePattern(utf8Slice("\\\\abc\\%"), utf8Slice("\\"));
        assertThat(likeVarchar(utf8Slice("\\abc%"), matcher)).isTrue();
    }

    @Test
    public void testAlternateEscapedCharacters()
    {
        LikePattern matcher = likePattern(utf8Slice("xxx%x_abcxx"), utf8Slice("x"));
        assertThat(likeVarchar(utf8Slice("x%_abcx"), matcher)).isTrue();
    }

    @Test
    public void testInvalidLikePattern()
    {
        assertThatThrownBy(() -> likePattern(utf8Slice("#"), utf8Slice("#")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> likePattern(utf8Slice("abc#abc"), utf8Slice("#")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> likePattern(utf8Slice("abc#"), utf8Slice("#")))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
    }

    @Test
    public void testIsLikePattern()
    {
        assertThat(isLikePattern(utf8Slice("abc"), Optional.empty())).isFalse();
        assertThat(isLikePattern(utf8Slice("abc#_def"), Optional.of(utf8Slice("#")))).isFalse();
        assertThat(isLikePattern(utf8Slice("abc##def"), Optional.of(utf8Slice("#")))).isFalse();
        assertThat(isLikePattern(utf8Slice("abc#%def"), Optional.of(utf8Slice("#")))).isFalse();
        assertThat(isLikePattern(utf8Slice("abc%def"), Optional.empty())).isTrue();
        assertThat(isLikePattern(utf8Slice("abcdef_"), Optional.empty())).isTrue();
        assertThat(isLikePattern(utf8Slice("abcdef##_"), Optional.of(utf8Slice("#")))).isTrue();
        assertThat(isLikePattern(utf8Slice("%abcdef#_"), Optional.of(utf8Slice("#")))).isTrue();
        assertThatThrownBy(() -> isLikePattern(utf8Slice("#"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> isLikePattern(utf8Slice("abc#abc"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> isLikePattern(utf8Slice("abc#"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
    }

    @Test
    public void testPatternConstantPrefixBytes()
    {
        assertThat(patternConstantPrefixBytes(utf8Slice("abc"), Optional.empty())).isEqualTo(3);
        assertThat(patternConstantPrefixBytes(utf8Slice("abc#_def"), Optional.of(utf8Slice("#")))).isEqualTo(8);
        assertThat(patternConstantPrefixBytes(utf8Slice("abc##def"), Optional.of(utf8Slice("#")))).isEqualTo(8);
        assertThat(patternConstantPrefixBytes(utf8Slice("abc#%def"), Optional.of(utf8Slice("#")))).isEqualTo(8);
        assertThat(patternConstantPrefixBytes(utf8Slice("abc%def"), Optional.empty())).isEqualTo(3);
        assertThat(patternConstantPrefixBytes(utf8Slice("abcdef_"), Optional.empty())).isEqualTo(6);
        assertThat(patternConstantPrefixBytes(utf8Slice("abcdef##_"), Optional.of(utf8Slice("#")))).isEqualTo(8);
        assertThat(patternConstantPrefixBytes(utf8Slice("%abcdef#_"), Optional.of(utf8Slice("#")))).isEqualTo(0);
        assertThatThrownBy(() -> patternConstantPrefixBytes(utf8Slice("#"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> patternConstantPrefixBytes(utf8Slice("abc#abc"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
        assertThatThrownBy(() -> patternConstantPrefixBytes(utf8Slice("abc#"), Optional.of(utf8Slice("#"))))
                .isInstanceOf(TrinoException.class)
                .hasMessage("Escape character must be followed by '%', '_' or the escape character itself");
    }

    @Test
    public void testUnescapeValidLikePattern()
    {
        assertThat(unescapeLiteralLikePattern(utf8Slice("abc"), Optional.empty())).isEqualTo(utf8Slice("abc"));
        assertThat(unescapeLiteralLikePattern(utf8Slice("abc#_"), Optional.of(utf8Slice("#")))).isEqualTo(utf8Slice("abc_"));
        assertThat(unescapeLiteralLikePattern(utf8Slice("a##bc#_"), Optional.of(utf8Slice("#")))).isEqualTo(utf8Slice("a#bc_"));
        assertThat(unescapeLiteralLikePattern(utf8Slice("a###_bc"), Optional.of(utf8Slice("#")))).isEqualTo(utf8Slice("a#_bc"));
    }

    @Test
    public void testLikeWithDynamicPattern()
    {
        assertThat(assertions.query("""
                SELECT value FROM (
                    VALUES
                        ('a', 'a'),
                        ('b', 'a'),
                        ('c', '%')) t(value, pattern)
                WHERE value LIKE pattern
                """))
                .matches("VALUES 'a', 'c'");

        assertThat(assertions.query("""
                SELECT value FROM (
                    VALUES
                        ('a%b', 'aX%b', 'X'),
                        ('a0b', 'aX%b', 'X'),
                        ('b_', 'aY_', 'Y'),
                        ('c%', 'cZ%', 'Z')) t(value, pattern, esc)
                WHERE value LIKE pattern ESCAPE esc
                """))
                .matches("VALUES 'a%b', 'c%'");
    }
}
