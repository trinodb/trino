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

import io.trino.Session;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static com.google.common.io.BaseEncoding.base16;
import static io.trino.spi.StandardErrorCode.JSON_INPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonArrayFunction
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
    public void testCreateEmptyArray()
    {
        assertThat(assertions.query(
                "SELECT json_array()"))
                .matches("VALUES VARCHAR '[]'");
    }

    @Test
    public void testMultipleElements()
    {
        assertThat(assertions.query(
                "SELECT json_array(1, true)"))
                .matches("VALUES VARCHAR '[1,true]'");
    }

    @Test
    public void testNullElement()
    {
        // ABSENT ON NULL is the default option
        assertThat(assertions.query(
                "SELECT json_array(null)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_array(null ABSENT ON NULL)"))
                .matches("VALUES VARCHAR '[]'");

        assertThat(assertions.query(
                "SELECT json_array(null NULL ON NULL)"))
                .matches("VALUES VARCHAR '[null]'");
    }

    @Test
    public void testDuplicateElement()
    {
        assertThat(assertions.query(
                "SELECT json_array(1, 1)"))
                .matches("VALUES VARCHAR '[1,1]'");
    }

    @Test
    public void testElementWithFormat()
    {
        // character string to be read as JSON
        assertThat(assertions.query(
                "SELECT json_array('{\"a\" : 1}' FORMAT JSON)"))
                .matches("VALUES VARCHAR '[{\"a\":1}]'");

        // binary string to be read as JSON
        byte[] bytes = "{\"a\" : 1}".getBytes(UTF_16LE);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_array(" + varbinaryLiteral + " FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES VARCHAR '[{\"a\":1}]'");

        // malformed string to be read as JSON
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_array('[...' FORMAT JSON)"))
                .hasErrorCode(JSON_INPUT_CONVERSION_ERROR);

        // duplicate key inside the formatted element: only one entry is retained
        assertThat(assertions.query(
                "SELECT json_array('{\"a\" : 1, \"a\" : 1}' FORMAT JSON)"))
                .matches("VALUES VARCHAR '[{\"a\":1}]'");
    }

    @Test
    public void testElementTypes()
    {
        assertThat(assertions.query(
                "SELECT json_array(1e0)"))
                .matches("VALUES VARCHAR '[1.0]'");

        // uuid can be cast to varchar
        assertThat(assertions.query(
                "SELECT json_array(UUID '12151fd2-7586-11e9-8f9e-2a86e4085a59')"))
                .matches("VALUES VARCHAR '[\"12151fd2-7586-11e9-8f9e-2a86e4085a59\"]'");

        // date can be cast to varchar
        assertThat(assertions.query(
                "SELECT json_array(DATE '2001-01-31')"))
                .matches("VALUES VARCHAR '[\"2001-01-31\"]'");

        // HyperLogLog cannot be cast to varchar
        assertTrinoExceptionThrownBy(() -> assertions.query(
                "SELECT json_array(approx_set(1))"))
                .hasErrorCode(NOT_SUPPORTED);
    }

    @Test
    public void testJsonReturningFunctionAsElement()
    {
        assertThat(assertions.query(
                "SELECT json_array(json_array(1))"))
                .matches("VALUES VARCHAR '[[1]]'");
    }

    @Test
    public void testSubqueries()
    {
        assertThat(assertions.query(
                "SELECT json_array((SELECT 1))"))
                .matches("VALUES VARCHAR '[1]'");
    }

    @Test
    public void testOutputFormat()
    {
        assertThat(assertions.query(
                "SELECT json_array(true)"))
                .matches("VALUES VARCHAR '[true]'");

        // default returned type
        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varchar)"))
                .matches("VALUES VARCHAR '[true]'");

        // default returned format
        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varchar FORMAT JSON)"))
                .matches("VALUES VARCHAR '[true]'");

        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varchar(100))"))
                .matches("VALUES CAST('[true]' AS varchar(100))");

        // varbinary output
        String output = "[true]";

        byte[] bytes = output.getBytes(UTF_8);
        String varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varbinary FORMAT JSON ENCODING UTF8)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(UTF_16LE);
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varbinary FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES " + varbinaryLiteral);

        bytes = output.getBytes(Charset.forName("UTF_32LE"));
        varbinaryLiteral = "X'" + base16().encode(bytes) + "'";
        assertThat(assertions.query(
                "SELECT json_array(true RETURNING varbinary FORMAT JSON ENCODING UTF32)"))
                .matches("VALUES " + varbinaryLiteral);
    }

    @Test
    public void testNestedAggregation()
    {
        assertThat(assertions.query("""
                SELECT json_array('x', max(a))
                FROM (VALUES ('abc'), ('def')) t(a)
                """))
                .matches("VALUES VARCHAR '[\"x\",\"def\"]'");
    }

    @Test
    public void testParameters()
    {
        Session session = Session.builder(assertions.getDefaultSession())
                .addPreparedStatement("my_query", "SELECT json_array(?, ?)")
                .build();
        assertThat(assertions.query(session, "EXECUTE my_query USING 'a', 1"))
                .matches("VALUES VARCHAR '[\"a\",1]'");
    }
}
