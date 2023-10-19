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

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.VarcharType.createVarcharType;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestUrlFunctions
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
    public void testUrlExtract()
    {
        validateUrlExtract("http://example.com/path1/p.php?k1=v1&k2=v2#Ref1", "http", "example.com", null, "/path1/p.php", "k1=v1&k2=v2", "Ref1");
        validateUrlExtract("http://example.com/path1/p.php?", "http", "example.com", null, "/path1/p.php", "", "");
        validateUrlExtract("http://example.com/path1/p.php", "http", "example.com", null, "/path1/p.php", "", "");
        validateUrlExtract("http://example.com:8080/path1/p.php?k1=v1&k2=v2#Ref1", "http", "example.com", 8080L, "/path1/p.php", "k1=v1&k2=v2", "Ref1");
        validateUrlExtract("https://username@example.com", "https", "example.com", null, "", "", "");
        validateUrlExtract("https://username:password@example.com", "https", "example.com", null, "", "", "");
        validateUrlExtract("mailto:test@example.com", "mailto", "", null, "", "", "");
        validateUrlExtract("foo", "", "", null, "foo", "", "");
        validateUrlExtract("http://example.com/^", null, null, null, null, null, null);
    }

    @Test
    public void testUrlExtractParameter()
    {
        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k1')"))
                .hasType(createVarcharType(53))
                .isEqualTo("v1");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k2')"))
                .hasType(createVarcharType(53))
                .isEqualTo("v2");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k3')"))
                .hasType(createVarcharType(53))
                .isEqualTo("");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k4')"))
                .hasType(createVarcharType(53))
                .isEqualTo("");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k2=v2&k3&k4#Ref1', 'k5')"))
                .isNull(createVarcharType(53));

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=v1&k1=v2&k1&k1#Ref1', 'k1')"))
                .hasType(createVarcharType(53))
                .isEqualTo("v1");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1&k1=v1&k1&k1#Ref1', 'k1')"))
                .hasType(createVarcharType(50))
                .isEqualTo("");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k=a=b=c&x=y#Ref1', 'k')"))
                .hasType(createVarcharType(47))
                .isEqualTo("a=b=c");

        assertThat(assertions.expression("url_extract_parameter('http://example.com/path1/p.php?k1=a%26k2%3Db&k2=c#Ref1', 'k2')"))
                .hasType(createVarcharType(54))
                .isEqualTo("c");

        assertThat(assertions.expression("url_extract_parameter('foo', 'k1')"))
                .isNull(createVarcharType(3));
    }

    @Test
    public void testUrlEncode()
    {
        String[][] outputInputPairs = {
                {"http%3A%2F%2Ftest", "http://test"},
                {"http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd", "http://test?a=b&c=d"},
                {"http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88", "http://\u30c6\u30b9\u30c8"},
                {"%7E%40%3A.-*_%2B+%E2%98%83", "~@:.-*_+ \u2603"},
                {"test", "test"},
        };

        for (String[] outputInputPair : outputInputPairs) {
            String input = outputInputPair[1];
            String output = outputInputPair[0];
            assertThat(assertions.function("url_encode", "'" + input + "'"))
                    .hasType(createVarcharType(input.length() * 12))
                    .isEqualTo(output);
        }

        assertThat(assertions.function("url_encode", "'\uD867\uDE3D'"))
                .hasType(createVarcharType(12))
                .isEqualTo("%F0%A9%B8%BD");
    }

    @Test
    public void testUrlDecode()
    {
        String[][] inputOutputPairs = {
                {"http%3A%2F%2Ftest", "http://test"},
                {"http%3A%2F%2Ftest%3Fa%3Db%26c%3Dd", "http://test?a=b&c=d"},
                {"http%3A%2F%2F%E3%83%86%E3%82%B9%E3%83%88", "http://\u30c6\u30b9\u30c8"},
                {"%7E%40%3A.-*_%2B+%E2%98%83", "~@:.-*_+ \u2603"},
                {"test", "test"},
        };

        for (String[] inputOutputPair : inputOutputPairs) {
            String input = inputOutputPair[0];
            String output = inputOutputPair[1];
            assertThat(assertions.function("url_decode", "'" + input + "'"))
                    .hasType(createVarcharType(input.length()))
                    .isEqualTo(output);
        }
    }

    private void validateUrlExtract(String url, String protocol, String host, Long port, String path, String query, String fragment)
    {
        assertThat(assertions.function("url_extract_protocol", "'" + url + "'"))
                .hasType(createVarcharType(url.length()))
                .isEqualTo(protocol);

        assertThat(assertions.function("url_extract_host", "'" + url + "'"))
                .hasType(createVarcharType(url.length()))
                .isEqualTo(host);

        assertThat(assertions.function("url_extract_port", "'" + url + "'"))
                .hasType(BIGINT)
                .isEqualTo(port);

        assertThat(assertions.function("url_extract_path", "'" + url + "'"))
                .hasType(createVarcharType(url.length()))
                .isEqualTo(path);

        assertThat(assertions.function("url_extract_query", "'" + url + "'"))
                .hasType(createVarcharType(url.length()))
                .isEqualTo(query);

        assertThat(assertions.function("url_extract_fragment", "'" + url + "'"))
                .hasType(createVarcharType(url.length()))
                .isEqualTo(fragment);
    }
}
