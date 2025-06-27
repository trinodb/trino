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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.JSON_OUTPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.PATH_EVALUATION_ERROR;
import static io.trino.spi.StandardErrorCode.SYNTAX_ERROR;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonQueryFunction
{
    private static final String INPUT = "[\"a\", \"b\", \"c\"]";
    private static final String OBJECT_INPUT = "{\"key\" : 1}";
    private static final String INCORRECT_INPUT = "[...";
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testJsonQuery()
    {
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $')"))
                .matches("VALUES VARCHAR '[\"a\",\"b\",\"c\"]'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $')"))
                .matches("VALUES VARCHAR '[\"a\",\"b\",\"c\"]'");

        // structural error not suppressed by the path engine in strict mode, and handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $[100]')"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $[100]' NULL ON ERROR)"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $[100]' EMPTY ARRAY ON ERROR)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $[100]' EMPTY OBJECT ON ERROR)"))
                .matches("VALUES VARCHAR '{}'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'strict $[100]' ERROR ON ERROR)"))
                .failure()
                .hasErrorCode(PATH_EVALUATION_ERROR)
                .hasMessage("path evaluation failed: structural error: invalid array subscript: [100, 100] for array of size 3");

        // structural error suppressed by the path engine in lax mode. empty sequence is returned, so ON EMPTY behavior is applied

        // default empty behavior is NULL ON EMPTY
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[100]')"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[100]' NULL ON EMPTY)"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[100]' EMPTY ARRAY ON EMPTY)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[100]' EMPTY OBJECT ON EMPTY)"))
                .matches("VALUES VARCHAR '{}'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[100]' ERROR ON EMPTY)"))
                .failure()
                .hasErrorCode(JSON_OUTPUT_CONVERSION_ERROR)
                .hasMessage("conversion from JSON failed: JSON path found no items");

        // path returns multiple items (no array wrapper specified). this case is handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 2]')"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 2]' NULL ON ERROR)"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 2]' EMPTY ARRAY ON ERROR)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 2]' EMPTY OBJECT ON ERROR)"))
                .matches("VALUES VARCHAR '{}'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 2]' ERROR ON ERROR)"))
                .failure()
                .hasErrorCode(JSON_OUTPUT_CONVERSION_ERROR)
                .hasMessage("conversion from JSON failed: JSON path found multiple items");
    }

    @Test
    public void testInputFormat()
    {
        // FORMAT JSON is default for character string input
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        // FORMAT JSON is the only supported format for character string input
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "' FORMAT JSON, 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "' FORMAT JSON ENCODING UTF8, 'lax $[1]')"))
                .failure().hasMessage("line 1:19: Cannot read input of type varchar(15) as JSON using formatting JSON ENCODING UTF8");

        // FORMAT JSON is default for binary string input
        byte[] bytes = INPUT.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_query(" + varbinaryLiteral + ", 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        assertThat(assertions.query(
                "SELECT json_query(" + varbinaryLiteral + " FORMAT JSON, 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        // FORMAT JSON ENCODING ... is supported for binary string input
        assertThat(assertions.query(
                "SELECT json_query(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        bytes = INPUT.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_query(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF16, 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        bytes = INPUT.getBytes(Charset.forName("UTF-32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_query(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF32, 'lax $[1]')"))
                .matches("VALUES VARCHAR '\"b\"'");

        // the encoding must match the actual data
        String finalVarbinaryLiteral = varbinaryLiteral;
        assertThat(assertions.query(
                "SELECT json_query(" + finalVarbinaryLiteral + " FORMAT JSON ENCODING UTF8, 'lax $[1]' ERROR ON ERROR)"))
                .failure().hasMessage("conversion to JSON failed: ");
    }

    @Test
    public void testInputConversionError()
    {
        // input conversion error is handled accordingly to the ON ERROR clause

        // default error behavior is NULL ON ERROR
        assertThat(assertions.query(
                "SELECT json_query('" + INCORRECT_INPUT + "', 'lax $[1]')"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INCORRECT_INPUT + "', 'lax $[1]' NULL ON ERROR)"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INCORRECT_INPUT + "', 'lax $[1]' EMPTY ARRAY ON ERROR)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_query('" + INCORRECT_INPUT + "', 'lax $[1]' EMPTY OBJECT ON ERROR)"))
                .matches("VALUES VARCHAR '{}'");

        assertThat(assertions.query(
                "SELECT json_query('" + INCORRECT_INPUT + "', 'lax $[1]' ERROR ON ERROR)"))
                .failure()
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");
    }

    @Test
    public void testPassingClause()
    {
        // watch out for case sensitive identifiers in JSON path
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $number + 1' PASSING 2 AS number)"))
                .failure().hasMessage("line 1:38: no value passed for parameter number. Try quoting \"number\" in the PASSING clause to match case");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $number + 1' PASSING 5 AS \"number\")"))
                .matches("VALUES VARCHAR '6'");

        // JSON parameter
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $array[0]' PASSING '[1, 2, 3]' FORMAT JSON AS \"array\")"))
                .matches("VALUES VARCHAR '1'");

        // input conversion error of JSON parameter is handled accordingly to the ON ERROR clause
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\")"))
                .matches("VALUES cast(null AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $array[0]' PASSING '[...' FORMAT JSON AS \"array\" ERROR ON ERROR)"))
                .failure()
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR)
                .hasMessage("conversion to JSON failed: ");

        // array index out of bounds
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[$number]' PASSING 5 AS \"number\")"))
                .matches("VALUES cast(null AS varchar)");

        // parameter cannot be converted to JSON -- returns null, because NULL ON ERROR is implicit
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $parameter' PASSING DATE '2001-01-31' AS \"parameter\")"))
                .matches("VALUES cast(null AS varchar)");

        // parameter cast to varchar
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $parameter' PASSING INTERVAL '2' DAY AS \"parameter\")"))
                .matches("VALUES cast('\"2 00:00:00.000\"' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $parameter' PASSING UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59' AS \"parameter\")"))
                .matches("VALUES cast('\"12151fd2-7586-11e9-8f9e-2a86e4085a59\"' AS varchar)");
    }

    @Test
    public void testOutput()
    {
        // default returned type is varchar, and default formatting is FORMAT JSON
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax 1')"))
                .matches("VALUES VARCHAR '1'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax true' RETURNING varchar FORMAT JSON)"))
                .matches("VALUES VARCHAR 'true'");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax null' RETURNING varchar FORMAT JSON)"))
                .matches("VALUES VARCHAR 'null'");

        // explicit returned type
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[1]' RETURNING char(10))"))
                .matches("VALUES cast('\"b\"' AS char(10))");

        // truncating cast from varchar to expected returned type
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax \"text too long\"' RETURNING char(10))"))
                .matches("VALUES cast('\"text too ' AS char(10))");

        // invalid returned type
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax 1' RETURNING tinyint)"))
                .failure().hasMessage("line 1:8: Cannot output JSON value as tinyint using formatting JSON");

        // returned type varbinary

        String output = "[\"a\",\"b\",\"c\"]"; // input without whitespace
        byte[] bytes = output.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        // default formatting is FORMAT JSON
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' RETURNING varbinary)"))
                .matches("VALUES " + varbinaryLiteral);

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' RETURNING varbinary FORMAT JSON)"))
                .matches("VALUES " + varbinaryLiteral);

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' RETURNING varbinary FORMAT JSON ENCODING UTF8)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' RETURNING varbinary FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(Charset.forName("UTF_32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' RETURNING varbinary FORMAT JSON ENCODING UTF32)"))
                .matches("VALUES " + varbinaryLiteral);
    }

    @Test
    public void testWrapperBehavior()
    {
        // by default, multiple output items cause error. the error is handled accordingly to the ON ERROR clause
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 1]')"))
                .matches("VALUES cast(null AS varchar)");

        // WITH CONDITIONAL ARRAY WRAPPER -> wrap results in array unless there is singleton JSON array or object
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 1]' WITH CONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[\"a\",\"b\"]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0]' WITH CONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[\"a\"]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' WITH CONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[\"a\",\"b\",\"c\"]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + OBJECT_INPUT + "', 'lax $' WITH CONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('{\"key\":1}' AS varchar)");

        // WITH UNCONDITIONAL ARRAY WRAPPER -> wrap results in array
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0 to 1]' WITH UNCONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[\"a\",\"b\"]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $[0]' WITH UNCONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[\"a\"]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' WITH UNCONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[[\"a\",\"b\",\"c\"]]' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + OBJECT_INPUT + "', 'lax $' WITH UNCONDITIONAL ARRAY WRAPPER)"))
                .matches("VALUES cast('[{\"key\":1}]' AS varchar)");
    }

    @Test
    public void testQuotesBehavior()
    {
        // when the result is a scalar string, the surrounding quptes are kept by default
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax \"some scalar text value\"')"))
                .matches("VALUES cast('\"some scalar text value\"' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax \"some scalar text value\"' KEEP QUOTES ON SCALAR STRING)"))
                .matches("VALUES cast('\"some scalar text value\"' AS varchar)");

        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax \"some scalar text value\"' OMIT QUOTES ON SCALAR STRING)"))
                .matches("VALUES cast('some scalar text value' AS varchar)");

        // the OMIT QUOTES clause does not affect nested string values
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $' OMIT QUOTES ON SCALAR STRING)"))
                .matches("VALUES cast('[\"a\",\"b\",\"c\"]' AS varchar)");
    }

    @Test
    public void testIncorrectPath()
    {
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'certainly not a valid path')"))
                .failure()
                .hasErrorCode(SYNTAX_ERROR)
                .hasMessage("line 1:39: mismatched input 'certainly' expecting {'lax', 'strict'}");
    }

    @Test
    public void testNullInput()
    {
        // null as input item
        assertThat(assertions.query(
                "SELECT json_query(null, 'lax $')"))
                .matches("VALUES cast(null AS varchar)");

        // null as SQL-value parameter is evaluated to a JSON null, and the corresponding returned SQL value is null
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $var' PASSING null AS \"var\")"))
                .matches("VALUES cast('null' AS varchar)");

        // null as JSON parameter is evaluated to empty sequence. this condition is handled accordingly to ON EMPTY behavior
        assertThat(assertions.query(
                "SELECT json_query('" + INPUT + "', 'lax $var' PASSING null FORMAT JSON AS \"var\" EMPTY ARRAY ON EMPTY)"))
                .matches("VALUES cast('[]' AS  varchar)");
    }

    @Test
    public void testDescendantMemberAccessorWithArrayWrapper()
    {
        assertThat(assertions.query(
                """
                SELECT json_query(
                                '{"a" : {"b" : 1}, "c" :  [true, {"c" : {"c" : null}}]}',
                                'lax $..c'
                                WITH ARRAY WRAPPER)
                """))
                .matches("""
                        VALUES cast('[[true,{"c":{"c":null}}],{"c":null},null]'AS varchar)
                        """);

        // `json_query` must return a single JSON item.
        // `WITH ARRAY WRAPPER` wraps `Diana` and `John` into a single JSON array, which can then be successfully returned.
        assertThat(assertions.query(
                """
                SELECT json_query(
                                '{"author":"Diana","id":{"value":1, "author":"John"},"notes":[{"type":1,"comment":"foo"},{"type":2,"comment":null}],"comment":["bar","baz"]}',
                                'lax $..author'
                                WITH ARRAY WRAPPER)
                """))
                .matches(
                        """
                        VALUES cast('["Diana","John"]'AS varchar)
                        """);
    }

    @Test
    public void testDescendantMemberAccessorWithoutArrayWrapper()
    {
        // Test `json_query` to find a non-existent member in the JSON.
        assertThat(assertions.query(
                """
                select json_query(
                                '{"id":{"value":1},"notes":[{"type":1,"comment":"foo"},{"type":2,"comment":null}],"comment":["bar","baz"]}',
                                'lax $..author'
                                omit quotes)
                """))
                .matches("VALUES cast(NULL as varchar)");

        // Test `json_query` to find a single descendant member in the JSON.
        assertThat(assertions.query(
                """
                select json_query(
                                '{"author":"Diana","id":{"value":1},"notes":[{"type":1,"comment":"foo"},{"type":2,"comment":null}],"comment":["bar","baz"]}',
                                'lax $..author'
                                omit quotes)
                """))
                .matches("VALUES cast('Diana' as varchar)");

        // Test `json_query` to find multiple descendant members in the JSON.
        // `json_query` must return a single JSON item.
        // `WITHOUT ARRAY WRAPPER` returns `NULL`, which is the default result in case of an error.
        assertThat(assertions.query(
                """
                select json_query(
                                '{"author":"Diana","id":{"value":1, "author":"John"},"notes":[{"type":1,"comment":"foo"},{"type":2,"comment":null}],"comment":["bar","baz"]}',
                                'lax $..author'
                                omit quotes)
                """))
                .matches("VALUES cast(NULL as varchar)");
    }
}
