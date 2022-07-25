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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonObjectFunction
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

    @Test
    public void testCreateEmptyObject()
    {
        assertThat(assertions.query(
                "SELECT json_object()"))
                .matches("VALUES VARCHAR '{}'");
    }

    @Test
    public void testArgumentPassingConventions()
    {
        assertThat(assertions.query(
                "SELECT json_object('X' : 'Y')"))
                .matches("VALUES VARCHAR '{\"X\":\"Y\"}'");

        assertThat(assertions.query(
                "SELECT json_object(KEY 'X' VALUE 'Y')"))
                .matches("VALUES VARCHAR '{\"X\":\"Y\"}'");

        assertThat(assertions.query(
                "SELECT json_object('X' VALUE 'Y')"))
                .matches("VALUES VARCHAR '{\"X\":\"Y\"}'");
    }

    @Test
    public void testMultipleMembers()
    {
        // the order of members in the result JSON object is arbitrary
        assertThat(assertions.query(
                "SELECT json_object('key_1' : 1, 'key_2' : 2)"))
                .matches("VALUES VARCHAR '{\"key_2\":2,\"key_1\":1}'");
    }

    @Test
    public void testNullKey()
    {
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object(CAST(null AS varchar) : 1)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("null value passed for JSON object key to JSON_OBJECT function");
    }

    @Test
    public void testNullValue()
    {
        // null value becomes JSON null
        assertThat(assertions.query(
                "SELECT json_object('key' : null NULL ON NULL)"))
                .matches("VALUES VARCHAR '{\"key\":null}'");

        // NULL ON NULL is the default option
        assertThat(assertions.query(
                "SELECT json_object('key' : null)"))
                .matches("VALUES VARCHAR '{\"key\":null}'");

        // member with null value is omitted
        assertThat(assertions.query(
                "SELECT json_object('key' : null ABSENT ON NULL)"))
                .matches("VALUES VARCHAR '{}'");
    }

    @Test
    public void testDuplicateKey()
    {
        // we don't support it because it requires creating a JSON object with duplicate key
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : 1, 'key' : 2 WITHOUT UNIQUE KEYS)"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("cannot construct a JSON object with duplicate key");

        // WITHOUT UNIQUE KEYS is the default option
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : 1, 'key' : 2)"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("cannot construct a JSON object with duplicate key");

        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : 1, 'key' : 2 WITH UNIQUE KEYS)"))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage("duplicate key passed to JSON_OBJECT function");
    }

    @Test
    public void testValueWithFormat()
    {
        // character string to be read as JSON
        assertThat(assertions.query(
                "SELECT json_object('key' : '[ 1, true, \"a\", null ]' FORMAT JSON)"))
                .matches("VALUES VARCHAR '{\"key\":[1,true,\"a\",null]}'");

        // binary string to be read as JSON
        byte[] bytes = "{\"a\" : 1}".getBytes(UTF_16LE);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_object('key' : " + varbinaryLiteral + " FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES VARCHAR '{\"key\":{\"a\":1}}'");

        // malformed string to be read as JSON
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : '[...' FORMAT JSON)"))
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR);

        // duplicate key inside the formatted value: only one entry is retained
        assertThat(assertions.query(
                "SELECT json_object('key' : '{\"a\" : 1, \"a\" : 1}' FORMAT JSON)"))
                .matches("VALUES VARCHAR '{\"key\":{\"a\":1}}'");

        assertThat(assertions.query(
                "SELECT json_object('key' : '{\"a\" : 1, \"a\" : 1}' FORMAT JSON WITHOUT UNIQUE KEYS)"))
                .matches("VALUES VARCHAR '{\"key\":{\"a\":1}}'");

        // in presence of input value with FORMAT, the option WITH UNIQUE KEYS is not supported, because the input function does not support this semantics
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : '{\"a\" : 1, \"a\" : 1}' FORMAT JSON WITH UNIQUE KEYS)"))
                .hasErrorCode(NOT_SUPPORTED)
                .hasMessage("line 1:8: WITH UNIQUE KEYS behavior is not supported for JSON_OBJECT function when input expression has FORMAT");
    }

    @Test
    public void testValueTypes()
    {
        assertThat(assertions.query(
                "SELECT json_object('key' : 1e0)"))
                .matches("VALUES VARCHAR '{\"key\":1.0}'");

        // uuid can be cast to varchar
        assertThat(assertions.query(
                "SELECT json_object('key' : UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')"))
                .matches("VALUES VARCHAR '{\"key\":\"12151fd2-7586-11e9-8f9e-2a86e4085a59\"}'");

        // date can be cast to varchar
        assertThat(assertions.query(
                "SELECT json_object('key' : DATE '2001-01-31')"))
                .matches("VALUES VARCHAR '{\"key\":\"2001-01-31\"}'");

        // HyperLogLog cannot be cast to varchar
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_object('key' : (approx_set(1)))"))
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testJsonReturningFunctionAsValue()
    {
        assertThat(assertions.query(
                "SELECT json_object('key' : json_object('a' : 1))"))
                .matches("VALUES VARCHAR '{\"key\":{\"a\":1}}'");
    }

    @Test
    public void testSubqueries()
    {
        assertThat(assertions.query(
                "SELECT json_object((SELECT 'key') : (SELECT 1))"))
                .matches("VALUES VARCHAR '{\"key\":1}'");
    }

    @Test
    public void testOutputFormat()
    {
        assertThat(assertions.query(
                "SELECT json_object('key' : 1)"))
                .matches("VALUES VARCHAR '{\"key\":1}'");

        // default returned type
        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varchar)"))
                .matches("VALUES VARCHAR '{\"key\":1}'");

        // default returned format
        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varchar FORMAT JSON)"))
                .matches("VALUES VARCHAR '{\"key\":1}'");

        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varchar(100))"))
                .matches("VALUES CAST('{\"key\":1}' AS varchar(100))");

        // varbinary output
        String output = "{\"key\":1}";

        byte[] bytes = output.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varbinary FORMAT JSON ENCODING UTF8)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varbinary FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(Charset.forName("UTF_32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_object('key' : 1 RETURNING varbinary FORMAT JSON ENCODING UTF32)"))
                .matches("VALUES " + varbinaryLiteral);
    }
}
