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
import io.trino.likematcher.LikeMatcher;
import io.trino.spi.TrinoException;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

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
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@TestInstance(PER_CLASS)
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
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("f%b__").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("foobar"), matcher));
        assertTrue(likeVarchar(offsetHeapSlice("foobar"), matcher));

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
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("f%b__").toStringUtf8(), Optional.empty());
        assertTrue(likeChar(6L, utf8Slice("foobar"), matcher));
        assertTrue(likeChar(6L, offsetHeapSlice("foobar"), matcher));
        assertTrue(likeChar(6L, utf8Slice("foob"), matcher));
        assertTrue(likeChar(6L, offsetHeapSlice("foob"), matcher));
        assertFalse(likeChar(7L, utf8Slice("foob"), matcher));
        assertFalse(likeChar(7L, offsetHeapSlice("foob"), matcher));

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
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("ala  ").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("ala  "), matcher));
        assertFalse(likeVarchar(utf8Slice("ala"), matcher));
    }

    @Test
    public void testLikeNewlineInPattern()
    {
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("%o\nbar").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), matcher));
    }

    @Test
    public void testLikeNewlineBeforeMatch()
    {
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("%b%").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), matcher));
    }

    @Test
    public void testLikeNewlineInMatch()
    {
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("f%b%").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("foo\nbar"), matcher));
    }

    @Test
    public void testLikeUtf8Pattern()
    {
        LikeMatcher matcher = likePattern(utf8Slice("%\u540d\u8a89%"), utf8Slice("\\"));
        assertFalse(likeVarchar(utf8Slice("foo"), matcher));
    }

    @Test
    public void testLikeInvalidUtf8Value()
    {
        Slice value = Slices.wrappedBuffer(new byte[] {'a', 'b', 'c', (byte) 0xFF, 'x', 'y'});
        LikeMatcher matcher = likePattern(utf8Slice("%b%"), utf8Slice("\\"));
        assertTrue(likeVarchar(value, matcher));
    }

    @Test
    public void testBackslashesNoSpecialTreatment()
    {
        LikeMatcher matcher = LikeMatcher.compile(utf8Slice("\\abc\\/\\\\").toStringUtf8(), Optional.empty());
        assertTrue(likeVarchar(utf8Slice("\\abc\\/\\\\"), matcher));
    }

    @Test
    public void testSelfEscaping()
    {
        LikeMatcher matcher = likePattern(utf8Slice("\\\\abc\\%"), utf8Slice("\\"));
        assertTrue(likeVarchar(utf8Slice("\\abc%"), matcher));
    }

    @Test
    public void testAlternateEscapedCharacters()
    {
        LikeMatcher matcher = likePattern(utf8Slice("xxx%x_abcxx"), utf8Slice("x"));
        assertTrue(likeVarchar(utf8Slice("x%_abcx"), matcher));
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
        assertFalse(isLikePattern(utf8Slice("abc"), Optional.empty()));
        assertFalse(isLikePattern(utf8Slice("abc#_def"), Optional.of(utf8Slice("#"))));
        assertFalse(isLikePattern(utf8Slice("abc##def"), Optional.of(utf8Slice("#"))));
        assertFalse(isLikePattern(utf8Slice("abc#%def"), Optional.of(utf8Slice("#"))));
        assertTrue(isLikePattern(utf8Slice("abc%def"), Optional.empty()));
        assertTrue(isLikePattern(utf8Slice("abcdef_"), Optional.empty()));
        assertTrue(isLikePattern(utf8Slice("abcdef##_"), Optional.of(utf8Slice("#"))));
        assertTrue(isLikePattern(utf8Slice("%abcdef#_"), Optional.of(utf8Slice("#"))));
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
        assertEquals(patternConstantPrefixBytes(utf8Slice("abc"), Optional.empty()), 3);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abc#_def"), Optional.of(utf8Slice("#"))), 8);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abc##def"), Optional.of(utf8Slice("#"))), 8);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abc#%def"), Optional.of(utf8Slice("#"))), 8);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abc%def"), Optional.empty()), 3);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abcdef_"), Optional.empty()), 6);
        assertEquals(patternConstantPrefixBytes(utf8Slice("abcdef##_"), Optional.of(utf8Slice("#"))), 8);
        assertEquals(patternConstantPrefixBytes(utf8Slice("%abcdef#_"), Optional.of(utf8Slice("#"))), 0);
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
        assertEquals(unescapeLiteralLikePattern(utf8Slice("abc"), Optional.empty()), utf8Slice("abc"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("abc#_"), Optional.of(utf8Slice("#"))), utf8Slice("abc_"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("a##bc#_"), Optional.of(utf8Slice("#"))), utf8Slice("a#bc_"));
        assertEquals(unescapeLiteralLikePattern(utf8Slice("a###_bc"), Optional.of(utf8Slice("#"))), utf8Slice("a#_bc"));
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
