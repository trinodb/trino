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

import java.nio.charset.StandardCharsets;

import static com.google.common.io.BaseEncoding.base16;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonConstructorFunction
{
    private static final String INPUT = "[\"a\", \"b\", \"c\"]";
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testJsonConstructor()
    {
        assertThat(assertions.query("SELECT JSON('" + INPUT + "')"))
                .matches("VALUES JSON '[\"a\",\"b\",\"c\"]'");

        assertThat(assertions.query("SELECT JSON(JSON '" + INPUT + "')"))
                .matches("VALUES JSON '[\"a\",\"b\",\"c\"]'");
    }

    @Test
    public void testJsonConstructorNullAndMalformedInput()
    {
        // NULL input -> NULL output.
        assertThat(assertions.query("SELECT JSON(CAST(NULL AS varchar))"))
                .matches("VALUES CAST(NULL AS json)");
        assertThat(assertions.query("SELECT JSON(CAST(NULL AS json))"))
                .matches("VALUES CAST(NULL AS json)");

        // Malformed input raises (JSON(...) has no ON ERROR clause; input is read with
        // failOnError=true).
        assertThat(assertions.query("SELECT JSON('not json')"))
                .failure()
                .hasMessageContaining("conversion to JSON failed");
        assertThat(assertions.query("SELECT JSON('[1, 2')"))
                .failure()
                .hasMessageContaining("conversion to JSON failed");
    }

    @Test
    public void testJsonConstructorDuplicateKeys()
    {
        // The {WITH | WITHOUT} UNIQUE KEYS clause is not implemented; duplicate keys are
        // preserved with their cardinality, as WITHOUT UNIQUE KEYS would. The constructor,
        // the JSON literal and json_parse all parse through the same value model, so they
        // agree: none collapses a repeated key.
        assertThat(assertions.query("SELECT json_format(JSON('{\"a\" : 1, \"a\" : 2}'))"))
                .matches("VALUES VARCHAR '{\"a\":1,\"a\":2}'");
        assertThat(assertions.query("SELECT json_format(JSON '{\"a\" : 1, \"a\" : 2}')"))
                .matches("VALUES VARCHAR '{\"a\":1,\"a\":2}'");
        assertThat(assertions.query("SELECT json_format(json_parse('{\"a\" : 1, \"a\" : 2}'))"))
                .matches("VALUES VARCHAR '{\"a\":1,\"a\":2}'");
    }

    @Test
    public void testJsonConstructorBinaryInput()
    {
        String utf16Literal = "X'" + base16().encode(INPUT.getBytes(UTF_16LE)) + "'";

        assertThat(assertions.query("SELECT JSON(" + utf16Literal + " FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES JSON '[\"a\",\"b\",\"c\"]'");

        String utf8Literal = "X'" + base16().encode(INPUT.getBytes(StandardCharsets.UTF_8)) + "'";
        assertThat(assertions.query("SELECT JSON(" + utf8Literal + " FORMAT JSON)"))
                .matches("VALUES JSON '[\"a\",\"b\",\"c\"]'");
    }
}
