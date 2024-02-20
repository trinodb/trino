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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.io.IOException;
import java.util.List;

import static io.trino.operator.scalar.JsonExtract.JsonExtractor;
import static io.trino.operator.scalar.JsonExtract.JsonValueJsonExtractor;
import static io.trino.operator.scalar.JsonExtract.ObjectFieldJsonExtractor;
import static io.trino.operator.scalar.JsonExtract.ScalarValueJsonExtractor;
import static io.trino.operator.scalar.JsonExtract.generateExtractor;
import static io.trino.plugin.base.util.JsonUtils.jsonFactory;
import static io.trino.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.assertions.TrinoExceptionAssert.assertTrinoExceptionThrownBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
public class TestJsonExtract
{
    @Test
    public void testJsonTokenizer()
    {
        assertThat(tokenizePath("$")).isEqualTo(ImmutableList.of());
        assertThat(tokenizePath("$")).isEqualTo(ImmutableList.of());
        assertThat(tokenizePath("$.foo")).isEqualTo(ImmutableList.of("foo"));
        assertThat(tokenizePath("$[\"foo\"]")).isEqualTo(ImmutableList.of("foo"));
        assertThat(tokenizePath("$[\"foo.bar\"]")).isEqualTo(ImmutableList.of("foo.bar"));
        assertThat(tokenizePath("$[42]")).isEqualTo(ImmutableList.of("42"));
        assertThat(tokenizePath("$.42")).isEqualTo(ImmutableList.of("42"));
        assertThat(tokenizePath("$.42.63")).isEqualTo(ImmutableList.of("42", "63"));
        assertThat(tokenizePath("$.foo.42.bar.63")).isEqualTo(ImmutableList.of("foo", "42", "bar", "63"));
        assertThat(tokenizePath("$.x.foo")).isEqualTo(ImmutableList.of("x", "foo"));
        assertThat(tokenizePath("$.x[\"foo\"]")).isEqualTo(ImmutableList.of("x", "foo"));
        assertThat(tokenizePath("$.x[42]")).isEqualTo(ImmutableList.of("x", "42"));
        assertThat(tokenizePath("$.foo_42._bar63")).isEqualTo(ImmutableList.of("foo_42", "_bar63"));
        assertThat(tokenizePath("$[foo_42][_bar63]")).isEqualTo(ImmutableList.of("foo_42", "_bar63"));
        assertThat(tokenizePath("$.foo:42.:bar63")).isEqualTo(ImmutableList.of("foo:42", ":bar63"));
        assertThat(tokenizePath("$[\"foo:42\"][\":bar63\"]")).isEqualTo(ImmutableList.of("foo:42", ":bar63"));

        assertPathToken("foo");

        assertQuotedPathToken("-1.1");
        assertQuotedPathToken("!@#$%^&*()[]{}/?'");
        assertQuotedPathToken("ab\u0001c");
        assertQuotedPathToken("ab\0c");
        assertQuotedPathToken("ab\t\n\rc");
        assertQuotedPathToken(".");
        assertQuotedPathToken("$");
        assertQuotedPathToken("]");
        assertQuotedPathToken("[");
        assertQuotedPathToken("'");
        assertQuotedPathToken("!@#$%^&*(){}[]<>?/|.,`~\r\n\t \0");
        assertQuotedPathToken("a\\\\b\\\"", "a\\b\"");

        // backslash not followed by valid escape
        assertInvalidPath("$[\"a\\ \"]");

        // colon in subscript must be quoted
        assertInvalidPath("$[foo:bar]");

        // whitespace is not allowed
        assertInvalidPath(" $.x");
        assertInvalidPath(" $.x ");
        assertInvalidPath("$. x");
        assertInvalidPath("$ .x");
        assertInvalidPath("$\n.x");
        assertInvalidPath("$.x [42]");
        assertInvalidPath("$.x[ 42]");
        assertInvalidPath("$.x[42 ]");
        assertInvalidPath("$.x[ \"foo\"]");
        assertInvalidPath("$.x[\"foo\" ]");
    }

    private static void assertPathToken(String fieldName)
    {
        assertThat(fieldName.indexOf('"') < 0).isTrue();
        assertThat(tokenizePath("$." + fieldName)).isEqualTo(ImmutableList.of(fieldName));
        assertThat(tokenizePath("$.foo." + fieldName + ".bar")).isEqualTo(ImmutableList.of("foo", fieldName, "bar"));
        assertPathTokenQuoting(fieldName);
    }

    private static void assertQuotedPathToken(String fieldName)
    {
        assertQuotedPathToken(fieldName, fieldName);
    }

    private static void assertQuotedPathToken(String fieldName, String expectedTokenizedField)
    {
        assertPathTokenQuoting(fieldName, expectedTokenizedField);
        // without quoting we should get an error
        assertInvalidPath("$." + fieldName);
    }

    private static void assertPathTokenQuoting(String fieldName)
    {
        assertPathTokenQuoting(fieldName, fieldName);
    }

    private static void assertPathTokenQuoting(String fieldName, String expectedTokenizedField)
    {
        assertThat(tokenizePath("$[\"" + fieldName + "\"]")).isEqualTo(ImmutableList.of(expectedTokenizedField));
        assertThat(tokenizePath("$.foo[\"" + fieldName + "\"].bar")).isEqualTo(ImmutableList.of("foo", expectedTokenizedField, "bar"));
    }

    public static void assertInvalidPath(String path)
    {
        assertTrinoExceptionThrownBy(() -> tokenizePath(path))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT);
    }

    @Test
    public void testScalarValueJsonExtractor()
            throws Exception
    {
        ScalarValueJsonExtractor extractor = new ScalarValueJsonExtractor();

        // Check scalar values
        assertThat(doExtract(extractor, "123")).isEqualTo("123");
        assertThat(doExtract(extractor, "-1")).isEqualTo("-1");
        assertThat(doExtract(extractor, "0.01")).isEqualTo("0.01");
        assertThat(doExtract(extractor, "\"abc\"")).isEqualTo("abc");
        assertThat(doExtract(extractor, "\"\"")).isEqualTo("");
        assertThat(doExtract(extractor, "null")).isEqualTo(null);

        // Test character escaped values
        assertThat(doExtract(extractor, "\"ab\\u0001c\"")).isEqualTo("ab\001c");
        assertThat(doExtract(extractor, "\"ab\\u0002c\"")).isEqualTo("ab\002c");

        // Complex types should return null
        assertThat(doExtract(extractor, "[1, 2, 3]")).isEqualTo(null);
        assertThat(doExtract(extractor, "{\"a\": 1}")).isEqualTo(null);
    }

    @Test
    public void testJsonValueJsonExtractor()
            throws Exception
    {
        JsonValueJsonExtractor extractor = new JsonValueJsonExtractor();

        // Check scalar values
        assertThat(doExtract(extractor, "123")).isEqualTo("123");
        assertThat(doExtract(extractor, "-1")).isEqualTo("-1");
        assertThat(doExtract(extractor, "0.01")).isEqualTo("0.01");
        assertThat(doExtract(extractor, "\"abc\"")).isEqualTo("\"abc\"");
        assertThat(doExtract(extractor, "\"\"")).isEqualTo("\"\"");
        assertThat(doExtract(extractor, "null")).isEqualTo("null");

        // Test character escaped values
        assertThat(doExtract(extractor, "\"ab\\u0001c\"")).isEqualTo("\"ab\\u0001c\"");
        assertThat(doExtract(extractor, "\"ab\\u0002c\"")).isEqualTo("\"ab\\u0002c\"");

        // Complex types should return json values
        assertThat(doExtract(extractor, "[1, 2, 3]")).isEqualTo("[1,2,3]");
        assertThat(doExtract(extractor, "{\"a\": 1}")).isEqualTo("{\"a\":1}");
    }

    @Test
    public void testArrayElementJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> firstExtractor = new ObjectFieldJsonExtractor<>("0", new ScalarValueJsonExtractor());
        ObjectFieldJsonExtractor<Slice> secondExtractor = new ObjectFieldJsonExtractor<>("1", new ScalarValueJsonExtractor());

        assertThat(doExtract(firstExtractor, "[]")).isEqualTo(null);
        assertThat(doExtract(firstExtractor, "[1, 2, 3]")).isEqualTo("1");
        assertThat(doExtract(secondExtractor, "[1, 2]")).isEqualTo("2");
        assertThat(doExtract(secondExtractor, "[1, null]")).isEqualTo(null);
        // Out of bounds
        assertThat(doExtract(secondExtractor, "[1]")).isEqualTo(null);
        // Check skipping complex structures
        assertThat(doExtract(secondExtractor, "[{\"a\": 1}, 2, 3]")).isEqualTo("2");
    }

    @Test
    public void testObjectFieldJsonExtractor()
            throws Exception
    {
        ObjectFieldJsonExtractor<Slice> extractor = new ObjectFieldJsonExtractor<>("fuu", new ScalarValueJsonExtractor());

        assertThat(doExtract(extractor, "{}")).isEqualTo(null);
        assertThat(doExtract(extractor, "{\"a\": 1}")).isEqualTo(null);
        assertThat(doExtract(extractor, "{\"fuu\": 1}")).isEqualTo("1");
        assertThat(doExtract(extractor, "{\"a\": 0, \"fuu\": 1}")).isEqualTo("1");
        // Check skipping complex structures
        assertThat(doExtract(extractor, "{\"a\": [1, 2, 3], \"fuu\": 1}")).isEqualTo("1");
    }

    @Test
    public void testFullScalarExtract()
    {
        assertThat(doScalarExtract("{}", "$")).isEqualTo(null);
        assertThat(doScalarExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu")).isEqualTo(null); // Null b/c value is complex type
        assertThat(doScalarExtract("{\"fuu\": 1}", "$.fuu")).isEqualTo("1");
        assertThat(doScalarExtract("{\"fuu\": 1}", "$[fuu]")).isEqualTo("1");
        assertThat(doScalarExtract("{\"fuu\": 1}", "$[\"fuu\"]")).isEqualTo("1");
        assertThat(doScalarExtract("{\"fuu\": null}", "$.fuu")).isEqualTo(null);
        assertThat(doScalarExtract("{\"fuu\": 1}", "$.bar")).isEqualTo(null);
        assertThat(doScalarExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]")).isEqualTo("\001"); // Test escaped characters
        assertThat(doScalarExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar")).isEqualTo("abc");
        assertThat(doScalarExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]")).isEqualTo("0.1");
        assertThat(doScalarExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]")).isEqualTo(null); // Null b/c value is complex type
        assertThat(doScalarExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]")).isEqualTo("101");
        assertThat(doScalarExtract("{\"fuu\": [0, {\"bar\": {\"key\" : [\"value\"]}}, 2]}", "$.fuu[1].bar.key[0]")).isEqualTo("value");

        // Test non-object extraction
        assertThat(doScalarExtract("[0, 1, 2]", "$[0]")).isEqualTo("0");
        assertThat(doScalarExtract("\"abc\"", "$")).isEqualTo("abc");
        assertThat(doScalarExtract("123", "$")).isEqualTo("123");
        assertThat(doScalarExtract("null", "$")).isEqualTo(null);

        // Test numeric path expression matches arrays and objects
        assertThat(doScalarExtract("[0, 1, 2]", "$.1")).isEqualTo("1");
        assertThat(doScalarExtract("[0, 1, 2]", "$[1]")).isEqualTo("1");
        assertThat(doScalarExtract("[0, 1, 2]", "$[\"1\"]")).isEqualTo("1");
        assertThat(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1")).isEqualTo("1");
        assertThat(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]")).isEqualTo("1");
        assertThat(doScalarExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]")).isEqualTo("1");

        // Test fields starting with a digit
        assertThat(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day")).isEqualTo("1");
        assertThat(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]")).isEqualTo("1");
        assertThat(doScalarExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]")).isEqualTo("1");
    }

    @Test
    public void testFullJsonExtract()
    {
        assertThat(doJsonExtract("{}", "$")).isEqualTo("{}");
        assertThat(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu")).isEqualTo("{\"bar\":1}");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$.fuu")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$[fuu]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": null}", "$.fuu")).isEqualTo("null");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$.bar")).isEqualTo(null);
        assertThat(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]")).isEqualTo("\"\\u0001\""); // Test escaped characters
        assertThat(doJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$.bar")).isEqualTo("\"abc\"");
        assertThat(doJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$.fuu[0]")).isEqualTo("0.1");
        assertThat(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1]")).isEqualTo("[100,101]");
        assertThat(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$.fuu[1][1]")).isEqualTo("101");

        // Test non-object extraction
        assertThat(doJsonExtract("[0, 1, 2]", "$[0]")).isEqualTo("0");
        assertThat(doJsonExtract("\"abc\"", "$")).isEqualTo("\"abc\"");
        assertThat(doJsonExtract("123", "$")).isEqualTo("123");
        assertThat(doJsonExtract("null", "$")).isEqualTo("null");

        // Test extraction using bracket json path
        assertThat(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"]")).isEqualTo("{\"bar\":1}");
        assertThat(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"][\"bar\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$[\"fuu\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": null}", "$[\"fuu\"]")).isEqualTo("null");
        assertThat(doJsonExtract("{\"fuu\": 1}", "$[\"bar\"]")).isEqualTo(null);
        assertThat(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]")).isEqualTo("\"\\u0001\""); // Test escaped characters
        assertThat(doJsonExtract("{\"fuu\": 1, \"bar\": \"abc\"}", "$[\"bar\"]")).isEqualTo("\"abc\"");
        assertThat(doJsonExtract("{\"fuu\": [0.1, 1, 2]}", "$[\"fuu\"][0]")).isEqualTo("0.1");
        assertThat(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1]")).isEqualTo("[100,101]");
        assertThat(doJsonExtract("{\"fuu\": [0, [100, 101], 2]}", "$[\"fuu\"][1][1]")).isEqualTo("101");

        // Test extraction using bracket json path with special json characters in path
        assertThat(doJsonExtract("{\"@$fuu\": {\".b.ar\": 1}}", "$[\"@$fuu\"]")).isEqualTo("{\".b.ar\":1}");
        assertThat(doJsonExtract("{\"fuu..\": 1}", "$[\"fuu..\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fu*u\": null}", "$[\"fu*u\"]")).isEqualTo("null");
        assertThat(doJsonExtract("{\",fuu\": 1}", "$[\"bar\"]")).isEqualTo(null);
        assertThat(doJsonExtract("{\",fuu\": [\"\\u0001\"]}", "$[\",fuu\"][0]")).isEqualTo("\"\\u0001\""); // Test escaped characters
        assertThat(doJsonExtract("{\":fu:u:\": 1, \":b:ar:\": \"abc\"}", "$[\":b:ar:\"]")).isEqualTo("\"abc\"");
        assertThat(doJsonExtract("{\"?()fuu\": [0.1, 1, 2]}", "$[\"?()fuu\"][0]")).isEqualTo("0.1");
        assertThat(doJsonExtract("{\"f?uu\": [0, [100, 101], 2]}", "$[\"f?uu\"][1]")).isEqualTo("[100,101]");
        assertThat(doJsonExtract("{\"fuu()\": [0, [100, 101], 2]}", "$[\"fuu()\"][1][1]")).isEqualTo("101");

        // Test extraction using mix of bracket and dot notation json path
        assertThat(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$[\"fuu\"].bar")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": {\"bar\": 1}}", "$.fuu[\"bar\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$[\"fuu\"][0]")).isEqualTo("\"\\u0001\""); // Test escaped characters
        assertThat(doJsonExtract("{\"fuu\": [\"\\u0001\"]}", "$.fuu[0]")).isEqualTo("\"\\u0001\""); // Test escaped characters

        // Test extraction using  mix of bracket and dot notation json path with special json characters in path
        assertThat(doJsonExtract("{\"@$fuu\": {\"bar\": 1}}", "$[\"@$fuu\"].bar")).isEqualTo("1");
        assertThat(doJsonExtract("{\",fuu\": {\"bar\": [\"\\u0001\"]}}", "$[\",fuu\"].bar[0]")).isEqualTo("\"\\u0001\""); // Test escaped characters

        // Test numeric path expression matches arrays and objects
        assertThat(doJsonExtract("[0, 1, 2]", "$.1")).isEqualTo("1");
        assertThat(doJsonExtract("[0, 1, 2]", "$[1]")).isEqualTo("1");
        assertThat(doJsonExtract("[0, 1, 2]", "$[\"1\"]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$.1")).isEqualTo("1");
        assertThat(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[1]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"0\" : 0, \"1\" : 1, \"2\" : 2, }", "$[\"1\"]")).isEqualTo("1");

        // Test fields starting with a digit
        assertThat(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$.30day")).isEqualTo("1");
        assertThat(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[30day]")).isEqualTo("1");
        assertThat(doJsonExtract("{\"15day\" : 0, \"30day\" : 1, \"90day\" : 2, }", "$[\"30day\"]")).isEqualTo("1");
    }

    @Test
    public void testInvalidExtracts()
    {
        assertInvalidExtract("", "", "Invalid JSON path: ''");
        assertInvalidExtract("{}", "$.bar[2][-1]", "Invalid JSON path: '$.bar[2][-1]'");
        assertInvalidExtract("{}", "$.fuu..bar", "Invalid JSON path: '$.fuu..bar'");
        assertInvalidExtract("{}", "$.", "Invalid JSON path: '$.'");
        assertInvalidExtract("", "$$", "Invalid JSON path: '$$'");
        assertInvalidExtract("", " ", "Invalid JSON path: ' '");
        assertInvalidExtract("", ".", "Invalid JSON path: '.'");
        assertInvalidExtract("{ \"store\": { \"book\": [{ \"title\": \"title\" }] } }", "$.store.book[", "Invalid JSON path: '$.store.book['");
    }

    @Test
    public void testExtractLongString()
    {
        String longString = "a".repeat(StreamReadConstraints.DEFAULT_MAX_STRING_LEN + 1);
        assertThat(doJsonExtract("{\"key\": \"" + longString + "\"}", "$.key")).isEqualTo('"' + longString + '"');
    }

    @Test
    public void testNoAutomaticEncodingDetection()
    {
        try (QueryAssertions assertions = new QueryAssertions()) {
            // Automatic encoding detection treats the following input as UTF-32
            assertThat(assertions.function("JSON_EXTRACT_SCALAR", "from_utf8(X'00 00 00 00 7b 22 72 22')", "'$.x'"))
                    .isNull(VARCHAR);
        }
    }

    private static String doExtract(JsonExtractor<Slice> jsonExtractor, String json)
            throws IOException
    {
        JsonFactory jsonFactory = jsonFactory();
        JsonParser jsonParser = jsonFactory.createParser(json);
        jsonParser.nextToken(); // Advance to the first token
        Slice extract = jsonExtractor.extract(jsonParser);
        return (extract == null) ? null : extract.toStringUtf8();
    }

    private static String doScalarExtract(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new ScalarValueJsonExtractor()));
        return (value == null) ? null : value.toStringUtf8();
    }

    private static String doJsonExtract(String inputJson, String jsonPath)
    {
        Slice value = JsonExtract.extract(Slices.utf8Slice(inputJson), generateExtractor(jsonPath, new JsonValueJsonExtractor()));
        return (value == null) ? null : value.toStringUtf8();
    }

    private static List<String> tokenizePath(String path)
    {
        return ImmutableList.copyOf(new JsonPathTokenizer(path));
    }

    private static void assertInvalidExtract(String inputJson, String jsonPath, String message)
    {
        assertTrinoExceptionThrownBy(() -> doJsonExtract(inputJson, jsonPath))
                .hasErrorCode(INVALID_FUNCTION_ARGUMENT)
                .hasMessage(message);
    }
}
