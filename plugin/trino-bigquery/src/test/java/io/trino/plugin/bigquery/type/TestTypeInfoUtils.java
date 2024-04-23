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
package io.trino.plugin.bigquery.type;

import org.junit.jupiter.api.Test;

import static io.trino.plugin.bigquery.type.TypeInfoUtils.parseTypeString;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestTypeInfoUtils
{
    @Test
    public void testBasicPrimitive()
    {
        assertTypeInfo("BOOL");
        assertTypeInfo("INT64");
        assertTypeInfo("FLOAT64");
        assertTypeInfo("STRING");
        assertTypeInfo("STRING(10)", "STRING");
        assertTypeInfo("BYTES");
        assertTypeInfo("DATE");
        assertTypeInfo("TIME");
        assertTypeInfo("DATETIME");
        assertTypeInfo("TIMESTAMP");
        assertTypeInfo("GEOGRAPHY");
        assertTypeInfo("JSON");
    }

    @Test
    public void testNumeric()
    {
        assertTypeInfo("NUMERIC", "NUMERIC(38, 9)");
        assertTypeInfo("NUMERIC(1)", "NUMERIC(1, 0)");
        assertTypeInfo("NUMERIC(5)", "NUMERIC(5, 0)");
        assertTypeInfo("NUMERIC(29)", "NUMERIC(29, 0)");
        assertTypeInfo("NUMERIC(1, 1)", "NUMERIC(1, 1)");
        assertTypeInfo("NUMERIC(10, 5)", "NUMERIC(10, 5)");
        assertTypeInfo("NUMERIC(38, 9)", "NUMERIC(38, 9)");

        assertThatThrownBy(() -> parseTypeString("NUMERIC(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 0");

        assertThatThrownBy(() -> parseTypeString("NUMERIC(39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 39");

        assertThatThrownBy(() -> parseTypeString("NUMERIC(38,39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal scale: 39");

        assertThatThrownBy(() -> parseTypeString("NUMERIC(4,5)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 4 is lower than scale 5");
    }

    @Test
    public void testBignumeric()
    {
        assertTypeInfo("BIGNUMERIC", "BIGNUMERIC(76, 38)");
        assertTypeInfo("BIGNUMERIC(1)", "BIGNUMERIC(1, 0)");
        assertTypeInfo("BIGNUMERIC(5)", "BIGNUMERIC(5, 0)");
        assertTypeInfo("BIGNUMERIC(38)", "BIGNUMERIC(38, 0)");
        assertTypeInfo("BIGNUMERIC(1, 1)", "BIGNUMERIC(1, 1)");
        assertTypeInfo("BIGNUMERIC(10, 5)", "BIGNUMERIC(10, 5)");
        assertTypeInfo("BIGNUMERIC(76, 38)", "BIGNUMERIC(76, 38)");

        assertThatThrownBy(() -> parseTypeString("BIGNUMERIC(0)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 0");

        assertThatThrownBy(() -> parseTypeString("BIGNUMERIC(77)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 77");

        assertThatThrownBy(() -> parseTypeString("BIGNUMERIC(76,39)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal scale: 39");

        assertThatThrownBy(() -> parseTypeString("BIGNUMERIC(4,5)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 4 is lower than scale 5");

        assertThatThrownBy(() -> parseTypeString("BIGNUMERIC(77)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("invalid decimal precision: 77");
    }

    @Test
    public void testArray()
    {
        assertTypeInfo("ARRAY<INT64>");
    }

    @Test
    public void testInvalidTypes()
    {
        // incomplete types should not be recognized as different types
        assertThatThrownBy(() -> parseTypeString("BOOLEAN"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of 'BOOLEAN' but 'BOOLEAN' is found.");
        assertThatThrownBy(() -> parseTypeString("INT"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of 'INT' but 'INT' is found.");
        assertThatThrownBy(() -> parseTypeString("TIMESTAMP WITH TIME ZONE"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: unexpected character at the end of 'TIMESTAMP WITH TIME ZONE'");
        assertThatThrownBy(() -> parseTypeString("ARRAY"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: < expected at the end of 'ARRAY'");
        assertThatThrownBy(() -> parseTypeString("STRUCT"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRUCT type is not supported, because it can contain unquoted field names, containing spaces, type names, and characters like '>'.");
        assertThatThrownBy(() -> parseTypeString("STRUCT<INT64, STRING>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRUCT type is not supported, because it can contain unquoted field names, containing spaces, type names, and characters like '>'.");
        assertThatThrownBy(() -> parseTypeString("STRUCT<# INT64>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("STRUCT type is not supported, because it can contain unquoted field names, containing spaces, type names, and characters like '>'.");

        // leading and trailing whitespace is not permitted
        assertThatThrownBy(() -> parseTypeString(" JSON"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of ' JSON' but ' ' is found.");
        assertThatThrownBy(() -> parseTypeString("JSON "))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: unexpected character at the end of 'JSON '");

        // invalid type parameters should not cause out of bounds errors
        assertThatThrownBy(() -> parseTypeString("STRING("))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("type parameters expected for type string STRING(");
        assertThatThrownBy(() -> parseTypeString("STRING()"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("type parameters expected for type string STRING()");
        assertThatThrownBy(() -> parseTypeString("STRING(1, 2)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type string only takes zero or one parameter, but 2 is seen");
        assertThatThrownBy(() -> parseTypeString("NUMERIC(1, 2, 3)"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Type decimal only takes two parameters, but 3 is seen");
        assertThatThrownBy(() -> parseTypeString("ARRAY<"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: type expected at the end of 'ARRAY<'");
        assertThatThrownBy(() -> parseTypeString("ARRAY<>"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 6 of 'ARRAY<>' but '>' is found.");

        // handle special characters
        assertThatThrownBy(() -> parseTypeString("()"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of '()' but '(' is found.");
        assertThatThrownBy(() -> parseTypeString("''"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of '''' but ''' is found.");
        assertThatThrownBy(() -> parseTypeString("łąka"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of 'łąka' but 'łąka' is found.");
        assertThatThrownBy(() -> parseTypeString("_"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of '_' but '_' is found.");
        assertThatThrownBy(() -> parseTypeString("#"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("Error: 'type' expected at the position 0 of '#' but '#' is found.");
    }

    private static void assertTypeInfo(String typeString)
    {
        assertThat(parseTypeString(typeString))
                .hasToString(typeString);
    }

    private static void assertTypeInfo(String typeString, String toString)
    {
        assertThat(parseTypeString(typeString))
                .hasToString(toString);
    }
}
