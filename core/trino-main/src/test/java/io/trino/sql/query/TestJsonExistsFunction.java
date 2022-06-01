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
package io.trino.sql.query;

import io.trino.json.PathEvaluationError;
import io.trino.operator.scalar.json.JsonInputConversionError;
import io.trino.sql.parser.ParsingException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonExistsFunction
{
    private static final String INPUT = "[\"a\", \"b\", \"c\"]";
    private static final String INCORRECT_INPUT = "[...";
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
    public void testJsonExists()
    {
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $[1]')"))
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[1]')"))
                .matches("VALUES true");

        // structural error suppressed by the path engine in lax mode. empty sequence is returned, so exists returns false
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $[100]')"))
                .matches("VALUES false");

        // structural error not suppressed by the path engine in strict mode, and handled accordingly to the ON ERROR clause

        // default error behavior is FALSE ON ERROR
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]')"))
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' TRUE ON ERROR)"))
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' FALSE ON ERROR)"))
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' UNKNOWN ON ERROR)"))
                .matches("VALUES cast(null AS boolean)");

        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INPUT + "', 'strict $[100]' ERROR ON ERROR)"))
                .isInstanceOf(PathEvaluationError.class)
                .hasMessage("path evaluation failed: structural error: invalid array subscript: [100, 100] for array of size 3");
    }

    @Test
    public void testInputFormat()
    {
        // FORMAT JSON is default for character string input
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $[1]')"))
                .matches("VALUES true");

        // FORMAT JSON is the only supported format for character string input
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "' FORMAT JSON, 'lax $[1]')"))
                .matches("VALUES true");

        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INPUT + "' FORMAT JSON ENCODING UTF8, 'lax $[1]')"))
                .hasMessage("line 1:20: Cannot read input of type varchar(15) as JSON using formatting JSON ENCODING UTF8");

        // FORMAT JSON is default for binary string input
        byte[] bytes = INPUT.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_exists(" + varbinaryLiteral + ", 'lax $[1]')"))
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON, 'lax $[1]')"))
                .matches("VALUES true");

        // FORMAT JSON ENCODING ... is supported for binary string input
        assertThat(assertions.query(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]')"))
                .matches("VALUES true");

        bytes = INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF16, 'lax $[1]')"))
                .matches("VALUES true");

        bytes = INPUT.getBytes(Charset.forName("UTF-32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_exists(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF32, 'lax $[1]')"))
                .matches("VALUES true");

        // the encoding must match the actual data
        String finalVarbinaryLiteral = varbinaryLiteral;
        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists(" + finalVarbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]' ERROR ON ERROR)"))
                .hasMessage("conversion to JSON failed: ");
    }

    @Test
    public void testInputConversionError()
    {
        // input conversion error is handled accordingly to the ON ERROR clause

        // default error behavior is FALSE ON ERROR
        assertThat(assertions.query(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'lax $[1]')"))
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' TRUE ON ERROR)"))
                .matches("VALUES true");

        assertThat(assertions.query(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' FALSE ON ERROR)"))
                .matches("VALUES false");

        assertThat(assertions.query(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' UNKNOWN ON ERROR)"))
                .matches("VALUES cast(null AS boolean)");

        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INCORRECT_INPUT + "', 'strict $[1]' ERROR ON ERROR)"))
                .isInstanceOf(JsonInputConversionError.class)
                .hasMessage("conversion to JSON failed: ");
    }

    @Test
    public void testPassingClause()
    {
        // watch out for case sensitive identifiers in JSON path
        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $number + 1' PASSING 2 AS number)"))
                .hasMessage("line 1:39: no value passed for parameter number. Try quoting \"number\" in the PASSING clause to match case");

        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $number + 1' PASSING 5 AS \"number\")"))
                .matches("VALUES true");

        // JSON parameter
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[1, 2, 3]' FORMAT JSON AS \"array\")"))
                .matches("VALUES true");

        // input conversion error of JSON parameter is handled accordingly to the ON ERROR clause
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\")"))
                .matches("VALUES false");

        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\" ERROR ON ERROR)"))
                .isInstanceOf(JsonInputConversionError.class)
                .hasMessage("conversion to JSON failed: ");

        // array index out of bounds
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $[$number]' PASSING 5 AS \"number\")"))
                .matches("VALUES false");
    }

    @Test
    public void testIncorrectPath()
    {
        assertThatThrownBy(() -> assertions.query(
                "SELECT json_exists('" + INPUT + "', 'certainly not a valid path')"))
                .isInstanceOf(ParsingException.class)
                .hasMessage("line 1:40: mismatched input 'certainly' expecting {'lax', 'strict'}");
    }

    @Test
    public void testNullInput()
    {
        // null as input item
        assertThat(assertions.query(
                "SELECT json_exists(null, 'lax $')"))
                .matches("VALUES cast(null AS boolean)");

        // null as SQL-value parameter is evaluated to a JSON null
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $var' PASSING null AS \"var\")"))
                .matches("VALUES true");

        // null as JSON parameter is evaluated to empty sequence
        assertThat(assertions.query(
                "SELECT json_exists('" + INPUT + "', 'lax $var' PASSING null FORMAT JSON AS \"var\")"))
                .matches("VALUES false");
    }
}
