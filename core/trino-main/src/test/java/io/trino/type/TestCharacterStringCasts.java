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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.operator.scalar.CharacterStringCasts.varcharToCharSaturatedFloorCast;
import static io.trino.operator.scalar.CharacterStringCasts.varcharToVarcharSaturatedFloorCast;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.testng.Assert.assertEquals;

@TestInstance(PER_CLASS)
public class TestCharacterStringCasts
{
    private static final String NON_BMP_CHARACTER = new String(Character.toChars(0x1F50D));

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

    @Test
    public void testVarcharToVarcharCast()
    {
        assertThat(assertions.expression("cast(a as varchar(20))")
                .binding("a", "'bar'"))
                .hasType(createVarcharType(20))
                .isEqualTo("bar");

        assertThat(assertions.expression("cast(cast(a as varchar(20)) as varchar(30))")
                .binding("a", "'bar'"))
                .hasType(createVarcharType(30))
                .isEqualTo("bar");

        assertThat(assertions.expression("cast(cast(a as varchar(20)) as varchar)")
                .binding("a", "'bar'"))
                .hasType(VARCHAR)
                .isEqualTo("bar");

        assertThat(assertions.expression("cast(a as varchar(3))")
                .binding("a", "'banana'"))
                .hasType(createVarcharType(3))
                .isEqualTo("ban");

        assertThat(assertions.expression("cast(cast(a as varchar(20)) as varchar(3))")
                .binding("a", "'banana'"))
                .hasType(createVarcharType(3))
                .isEqualTo("ban");
    }

    @Test
    public void testVarcharToCharCast()
    {
        assertThat(assertions.expression("cast(a as char(10))")
                .binding("a", "'bar  '"))
                .hasType(createCharType(10))
                .isEqualTo("bar       ");

        assertThat(assertions.expression("cast(a as char)")
                .binding("a", "'bar'"))
                .hasType(createCharType(1))
                .isEqualTo("b");

        assertThat(assertions.expression("cast(a as char)")
                .binding("a", "'   '"))
                .hasType(createCharType(1))
                .isEqualTo(" ");
    }

    @Test
    public void testCharToVarcharCast()
    {
        assertThat(assertions.expression("cast(cast(a as char(5)) as varchar(10))")
                .binding("a", "'bar'"))
                .hasType(createVarcharType(10))
                .isEqualTo("bar  ");

        assertThat(assertions.expression("cast(cast(a as char(5)) as varchar(1))")
                .binding("a", "'bar'"))
                .hasType(createVarcharType(1))
                .isEqualTo("b");

        assertThat(assertions.expression("cast(cast(a as char(5)) as varchar(2))")
                .binding("a", "'b'"))
                .hasType(createVarcharType(2))
                .isEqualTo("b ");

        assertThat(assertions.expression("cast(cast(a as char(5)) as varchar(1))")
                .binding("a", "'b'"))
                .hasType(createVarcharType(1))
                .isEqualTo("b");

        assertThat(assertions.expression("cast(cast(a as char(3)) as varchar(3))")
                .binding("a", "'bar'"))
                .hasType(createVarcharType(3))
                .isEqualTo("bar");

        assertThat(assertions.expression("cast(cast(a as char(3)) as varchar(3))")
                .binding("a", "'b'"))
                .hasType(createVarcharType(3))
                .isEqualTo("b  ");
    }

    @Test
    public void testVarcharToCharSaturatedFloorCast()
    {
        String nonBmpCharacterMinusOne = new String(Character.toChars(0x1F50C));
        String maxCodePoint = new String(Character.toChars(Character.MAX_CODE_POINT));
        String codePointBeforeSpace = new String(Character.toChars(' ' - 1));

        assertEquals(varcharToCharSaturatedFloorCast(
                5L,
                utf8Slice("123" + new String(Character.toChars(0xE000)))),
                utf8Slice("123" + new String(Character.toChars(0xD7FF)) + maxCodePoint));

        // Truncation
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("12345")),
                utf8Slice("1234"));

        // Size fits, preserved
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("1234")),
                utf8Slice("1234"));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("123" + NON_BMP_CHARACTER)),
                utf8Slice("123" + NON_BMP_CHARACTER));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("12" + NON_BMP_CHARACTER + "3")),
                utf8Slice("12" + NON_BMP_CHARACTER + "3"));

        // Size fits, preserved except char(4) representation has trailing spaces removed
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("123 ")),
                utf8Slice("123"));

        // Too short, casted back would be padded with ' ' and thus made greater (VarcharOperators.lessThan), so last character needs decrementing
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("123")),
                utf8Slice("122" + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("12 ")),
                utf8Slice("12" + codePointBeforeSpace + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("1  ")),
                utf8Slice("1 " + codePointBeforeSpace + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice(" ")),
                utf8Slice(codePointBeforeSpace + maxCodePoint + maxCodePoint + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("12" + NON_BMP_CHARACTER)),
                utf8Slice("12" + nonBmpCharacterMinusOne + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("1" + NON_BMP_CHARACTER + "3")),
                utf8Slice("1" + NON_BMP_CHARACTER + "2" + maxCodePoint));

        // Too short, casted back would be padded with ' ' and thus made greater (VarcharOperators.lessThan), previous to last needs decrementing since last is \0
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("12\0")),
                utf8Slice("11" + maxCodePoint + maxCodePoint));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("1\0")),
                utf8Slice("0" + maxCodePoint + maxCodePoint + maxCodePoint));

        // Smaller than any char(4) casted back to varchar, so the result is lowest char(4) possible
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("\0")),
                utf8Slice("\0\0\0\0"));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("\0\0")),
                utf8Slice("\0\0\0\0"));
        assertEquals(varcharToCharSaturatedFloorCast(
                4L,
                utf8Slice("")),
                utf8Slice("\0\0\0\0"));
    }

    @Test
    public void testVarcharToVarcharSaturatedFloorCast()
    {
        assertVarcharToVarcharSaturatedFloorCast(4L, "12345", "1234");
        assertVarcharToVarcharSaturatedFloorCast(5L, "12345", "12345");
        assertVarcharToVarcharSaturatedFloorCast(6L, "12345", "12345");

        assertVarcharToVarcharSaturatedFloorCast(4L, "123  ", "123 ");
        assertVarcharToVarcharSaturatedFloorCast(5L, "123  ", "123  ");

        assertVarcharToVarcharSaturatedFloorCast(4L, "1234" + NON_BMP_CHARACTER, "1234");
        assertVarcharToVarcharSaturatedFloorCast(5L, "1234" + NON_BMP_CHARACTER, "1234" + NON_BMP_CHARACTER);
        assertVarcharToVarcharSaturatedFloorCast(6L, "1234" + NON_BMP_CHARACTER, "1234" + NON_BMP_CHARACTER);
    }

    private void assertVarcharToVarcharSaturatedFloorCast(long length, String baseString, String expected)
    {
        assertThat(varcharToVarcharSaturatedFloorCast(length, utf8Slice(baseString)))
                .isEqualTo(utf8Slice(expected));
    }
}
