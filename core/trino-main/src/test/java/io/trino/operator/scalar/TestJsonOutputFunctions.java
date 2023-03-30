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

import io.trino.spi.type.SqlVarbinary;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.nio.charset.Charset;

import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestJsonOutputFunctions
{
    private static final String JSON_EXPRESSION = "\"$varchar_to_json\"('{\"key1\" : 1e0, \"key2\" : true, \"key3\" : null}', true)";
    private static final String OUTPUT = "{\"key1\":1.0,\"key2\":true,\"key3\":null}";

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
    public void testJsonToVarchar()
    {
        assertThat(assertions.expression("\"$json_to_varchar\"(" + JSON_EXPRESSION + ", TINYINT '1', true)"))
                .hasType(VARCHAR)
                .isEqualTo(OUTPUT);
    }

    @Test
    public void testJsonToVarbinaryUtf8()
    {
        assertThat(assertions.expression("\"$json_to_varbinary\"(" + JSON_EXPRESSION + ", TINYINT '1', true)"))
                .isEqualTo(new SqlVarbinary(OUTPUT.getBytes(UTF_8)));

        assertThat(assertions.expression("\"$json_to_varbinary_utf8\"(" + JSON_EXPRESSION + ", TINYINT '1', true)"))
                .isEqualTo(new SqlVarbinary(OUTPUT.getBytes(UTF_8)));
    }

    @Test
    public void testJsonToVarbinaryUtf16()
    {
        assertThat(assertions.expression("\"$json_to_varbinary_utf16\"(" + JSON_EXPRESSION + ", TINYINT '1', true)"))
                .isEqualTo(new SqlVarbinary(OUTPUT.getBytes(UTF_16LE)));
    }

    @Test
    public void testJsonToVarbinaryUtf32()
    {
        assertThat(assertions.expression("\"$json_to_varbinary_utf32\"(" + JSON_EXPRESSION + ", TINYINT '1', true)"))
                .isEqualTo(new SqlVarbinary(OUTPUT.getBytes(Charset.forName("UTF-32LE"))));
    }

    @Test
    public void testQuotesBehavior()
    {
        // keep quotes on scalar string
        assertThat(assertions.expression("\"$json_to_varchar\"(\"$varchar_to_json\"('\"some_text\"', true), TINYINT '1', false)"))
                .hasType(VARCHAR)
                .isEqualTo("\"some_text\"");

        // omit quotes on scalar string
        assertThat(assertions.expression("\"$json_to_varchar\"(\"$varchar_to_json\"('\"some_text\"', true), TINYINT '1', true)"))
                .hasType(VARCHAR)
                .isEqualTo("some_text");

        // quotes behavior does not apply to nested string. the quotes are preserved
        assertThat(assertions.expression("\"$json_to_varchar\"(\"$varchar_to_json\"('[\"some_text\"]', true), TINYINT '1', true)"))
                .hasType(VARCHAR)
                .isEqualTo("[\"some_text\"]");
    }

    @Test
    public void testNullInput()
    {
        assertThat(assertions.expression("\"$json_to_varchar\"(null, TINYINT '1', true)"))
                .isNull(VARCHAR);
    }
}
