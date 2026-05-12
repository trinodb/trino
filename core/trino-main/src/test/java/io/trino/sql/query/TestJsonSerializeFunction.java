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
public class TestJsonSerializeFunction
{
    private static final String INPUT = "[\"a\", \"b\", \"c\"]";
    private static final String SERIALIZED_INPUT = "[\"a\",\"b\",\"c\"]";
    private final QueryAssertions assertions = new QueryAssertions();

    @AfterAll
    public void teardown()
    {
        assertions.close();
    }

    @Test
    public void testJsonSerialize()
    {
        assertThat(assertions.query("SELECT JSON_SERIALIZE('" + INPUT + "')"))
                .matches("VALUES VARCHAR '[\"a\",\"b\",\"c\"]'");

        assertThat(assertions.query("SELECT JSON_SERIALIZE(JSON '" + INPUT + "')"))
                .matches("VALUES VARCHAR '[\"a\",\"b\",\"c\"]'");

        assertThat(assertions.query("SELECT JSON_SERIALIZE(JSON_QUERY('" + INPUT + "', 'lax $[1]'))"))
                .matches("VALUES VARCHAR '\"b\"'");

        assertThat(assertions.query("SELECT JSON_SERIALIZE(JSON_QUERY('" + INPUT + "', 'lax $[1]' RETURNING JSON))"))
                .matches("VALUES VARCHAR '\"b\"'");
    }

    @Test
    public void testJsonSerializeNullAndMalformedInput()
    {
        // NULL input -> NULL output (independent of the ON ERROR clause).
        assertThat(assertions.query("SELECT JSON_SERIALIZE(CAST(NULL AS varchar))"))
                .matches("VALUES CAST(NULL AS varchar)");
        assertThat(assertions.query("SELECT JSON_SERIALIZE(CAST(NULL AS json))"))
                .matches("VALUES CAST(NULL AS varchar)");

        // The ON ERROR clause is a Trino extension (SQL:2023 §6.37 JSON_SERIALIZE has no ON
        // ERROR clause); the default is ERROR ON ERROR — malformed input raises.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('not json')"))
                .failure()
                .hasMessageContaining("conversion to JSON failed");
        assertThat(assertions.query("SELECT JSON_SERIALIZE('[1, 2')"))
                .failure()
                .hasMessageContaining("conversion to JSON failed");

        // Explicit ERROR ON ERROR has the same behavior.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('not json' ERROR ON ERROR)"))
                .failure()
                .hasMessageContaining("conversion to JSON failed");
    }

    @Test
    public void testJsonSerializeNullOnError()
    {
        // NULL ON ERROR turns the parse / conversion failure into SQL NULL instead of an error.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('not json' NULL ON ERROR)"))
                .matches("VALUES CAST(NULL AS varchar)");
        assertThat(assertions.query("SELECT JSON_SERIALIZE('[1, 2' NULL ON ERROR)"))
                .matches("VALUES CAST(NULL AS varchar)");

        // Valid input under NULL ON ERROR still produces the serialized text.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('[1, 2, 3]' NULL ON ERROR)"))
                .matches("VALUES VARCHAR '[1,2,3]'");

        // NULL ON ERROR combined with RETURNING.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('not json' RETURNING varchar NULL ON ERROR)"))
                .matches("VALUES CAST(NULL AS varchar)");
    }

    @Test
    public void testJsonSerializeReturningCharTruncation()
    {
        // RETURNING char(N) where N is shorter than the serialized text.
        // char(N) pads on the right with spaces and truncates if longer.
        assertThat(assertions.query("SELECT JSON_SERIALIZE('" + INPUT + "' RETURNING char(50))"))
                .matches("VALUES cast('[\"a\",\"b\",\"c\"]' AS char(50))");
    }

    @Test
    public void testJsonSerializeReturningBinary()
    {
        byte[] utf16Bytes = INPUT.getBytes(UTF_16LE);
        String utf16Literal = "X'" + base16().encode(utf16Bytes) + "'";

        assertThat(assertions.query("SELECT JSON_SERIALIZE(" + utf16Literal + " FORMAT JSON ENCODING UTF16 RETURNING varbinary FORMAT JSON ENCODING UTF8)"))
                .matches("VALUES to_utf8('[\"a\",\"b\",\"c\"]')");

        byte[] utf8Bytes = INPUT.getBytes(StandardCharsets.UTF_8);
        String utf8Literal = "X'" + base16().encode(utf8Bytes) + "'";
        String expectedUtf16Literal = "X'" + base16().encode(SERIALIZED_INPUT.getBytes(UTF_16LE)) + "'";

        assertThat(assertions.query("SELECT JSON_SERIALIZE(" + utf8Literal + " FORMAT JSON RETURNING varbinary FORMAT JSON ENCODING UTF16)"))
                .matches("VALUES " + expectedUtf16Literal);
    }
}
