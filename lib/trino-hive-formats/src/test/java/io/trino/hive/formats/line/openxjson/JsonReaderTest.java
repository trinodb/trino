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
package io.trino.hive.formats.line.openxjson;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.starburst.openjson.JSONArray;
import io.starburst.openjson.JSONException;
import io.starburst.openjson.JSONObject;
import io.starburst.openjson.JSONTokener;
import org.testng.annotations.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class JsonReaderTest
{
    @Test
    public void testJsonNull()
            throws InvalidJsonException
    {
        assertJsonValue("null", null);
    }

    @Test
    public void testJsonPrimitive()
            throws InvalidJsonException
    {
        // unquoted values
        assertJsonValue("true", new JsonString("true", false));
        assertJsonValue("false", new JsonString("false", false));
        assertJsonValue("TRUE", new JsonString("TRUE", false));
        assertJsonValue("FALSE", new JsonString("FALSE", false));
        assertJsonValue("42", new JsonString("42", false));
        assertJsonValue("1.23", new JsonString("1.23", false));
        assertJsonValue("1.23e10", new JsonString("1.23e10", false));
        assertJsonValue("1.23E10", new JsonString("1.23E10", false));
        assertJsonValue("Infinity", new JsonString("Infinity", false));
        assertJsonValue("NaN", new JsonString("NaN", false));
        assertJsonValue("abc", new JsonString("abc", false));

        // anything is allowed after the value ends, which requires a separator
        assertJsonValue("true;anything", new JsonString("true", false));
        assertJsonValue("false anything", new JsonString("false", false));

        // Quoted string values
        assertJsonValue("\"\"", new JsonString("", true));
        assertJsonValue("\"abc\"", new JsonString("abc", true));

        // escapes
        assertJsonValue("\" \\\\ \\t \\b \\n \\r \\f \\a \\v \\u1234 \\uFFFD \\ufffd \"",
                new JsonString(" \\ \t \b \n \r \f \007 \011 \u1234 \uFFFD \ufffd ", true));

        // any other character is just passed through
        assertJsonValue("\"\\X\"", new JsonString("X", true));
        assertJsonValue("\"\\\"\"", new JsonString("\"", true));
        assertJsonValue("\"\\'\"", new JsonString("'", true));

        // unterminated escapes are an error
        assertJsonFails("\"\\\"");
        assertJsonFails("\"\\u1\"");
        assertJsonFails("\"\\u12\"");
        assertJsonFails("\"\\u123\"");

        // unicode escape requires hex
        assertJsonFails("\"\\u123X\"");

        // unterminated string is an error
        assertJsonFails("\"abc");
        assertJsonFails("\"a\\tc");

        // anything is allowed after the value
        assertJsonValue("\"abc\"anything", new JsonString("abc", true));
    }

    @Test
    public void testJsonObject()
            throws InvalidJsonException
    {
        assertJsonValue("{}", ImmutableMap.of());
        assertJsonValue("{ }", ImmutableMap.of());
        assertJsonFails("{");
        assertJsonFails("{{");

        // anything is allowed after the object
        assertJsonValue("{}anything allowed", ImmutableMap.of());

        assertJsonValue("{ \"a\" : 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonValue("{ \"a\" = 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonValue("{ \"a\" => 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonValue("{ a : 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonValue("{ a = 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonValue("{ a => 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));
        assertJsonFails("{ \"a\"");
        assertJsonFails("{ a");
        assertJsonFails("{ \"a\",");
        assertJsonFails("{ \"a\";");
        assertJsonFails("{ \"a\",2.34");
        assertJsonFails("{ \"a\";2.34");
        assertJsonFails("{ a x 2.34 }");
        assertJsonFails("{ a ~ 2.34 }");
        assertJsonFails("{ a -> 2.34 }");
        // starburst allows for :> due to a bug, but the original code did not support this
        assertJsonTrinoFails("{ a :> 2.34 }");
        assertJsonHive("{ a :> 2.34 }", ImmutableMap.of("a", new JsonString("2.34", false)));

        assertJsonValue("{ a : 2.34 , b : false}", ImmutableMap.of("a", new JsonString("2.34", false), "b", new JsonString("false", false)));
        assertJsonValue("{ a : 2.34 ; b : false}", ImmutableMap.of("a", new JsonString("2.34", false), "b", new JsonString("false", false)));
        assertJsonFails("{ a : 2.34 x b : false}");
        assertJsonFails("{ a : 2.34 ^ b : false}");
        assertJsonFails("{ a : 2.34 : b : false}");

        assertJsonValue("{ a : NaN }", ImmutableMap.of("a", new JsonString("NaN", false)));
        assertJsonValue("{ a : \"NaN\" }", ImmutableMap.of("a", new JsonString("NaN", true)));

        // Starburst hive does not allow unquoted field names, but Trino and the original code does
        assertJsonTrinoOnly("{ true : NaN }", ImmutableMap.of("true", new JsonString("NaN", false)));
        assertJsonTrinoOnly("{ 123 : NaN }", ImmutableMap.of("123", new JsonString("NaN", false)));

        // field name can not be null
        assertJsonFails("{ null : \"NaN\" }");
        // field name can not be structural
        assertJsonFails("{ [] : \"NaN\" }");
        assertJsonFails("{ {} : \"NaN\" }");

        // map can contain c-style comments
        assertJsonValue("/*foo*/{/*foo*/\"a\"/*foo*/:/*foo*/2.34/*foo*/}/*foo*/", ImmutableMap.of("a", new JsonString("2.34", false)));
        // unterminated comment is an error
        assertJsonFails("/*foo*/{/*foo*/\"a\"/*foo*/:/*foo");
        // end of line comments are always an error since the value is unterminated
        assertJsonFails("/*foo*/{/*foo*/\"a\"/*foo*/:#end-of-line");
        assertJsonFails("/*foo*/{/*foo*/\"a\"/*foo*/://end-of-line");
    }

    @Test
    public void testJsonArray()
            throws InvalidJsonException
    {
        assertJsonValue("[]", ImmutableList.of());
        assertJsonValue("[,]", Arrays.asList(null, null));
        assertJsonFails("[");
        assertJsonFails("[42");
        assertJsonFails("[42,");

        // anything is allowed after the array
        assertJsonValue("[]anything allowed", ImmutableList.of());

        assertJsonValue("[ 2.34 ]", singletonList(new JsonString("2.34", false)));
        assertJsonValue("[ NaN ]", singletonList(new JsonString("NaN", false)));
        assertJsonValue("[ \"NaN\" ]", singletonList(new JsonString("NaN", true)));

        assertJsonValue("[ 2.34 , ]", Arrays.asList(new JsonString("2.34", false), null));
        assertJsonValue("[ NaN , ]", Arrays.asList(new JsonString("NaN", false), null));
        assertJsonValue("[ \"NaN\" , ]", Arrays.asList(new JsonString("NaN", true), null));

        // map can contain c-style comments
        assertJsonValue("/*foo*/[/*foo*/\"a\"/*foo*/,/*foo*/2.34/*foo*/]/*foo*/", ImmutableList.of(new JsonString("a", true), new JsonString("2.34", false)));
        // unterminated comment is an error
        assertJsonFails("/*foo*/[/*foo*/\"a\"/*foo*/,/*foo");
        // end of line comments are always an error since the value is unterminated
        assertJsonFails("/*foo*/[/*foo*/\"a\"/*foo*/,#end-of-line");
        assertJsonFails("/*foo*/[/*foo*/\"a\"/*foo*/,//end-of-line");
    }

    private static void assertJsonValue(String json, Object expectedTrinoValue)
            throws InvalidJsonException
    {
        assertJsonTrino(json, expectedTrinoValue);
        assertJsonHive(json, expectedTrinoValue);
    }

    private static void assertJsonTrinoOnly(String json, Object expected)
            throws InvalidJsonException
    {
        assertJsonTrino(json, expected);
        assertJsonHiveFails(json);
    }

    private static void assertJsonTrino(String json, Object expected)
            throws InvalidJsonException
    {
        assertThat(JsonReader.readJson(json, Function.identity()))
                .isEqualTo(expected);
    }

    private static void assertJsonHive(String json, Object expectedTrinoValue)
    {
        Object actualHiveValue = unwrapHiveValue(new JSONTokener(false, json).nextValue());
        Object expectedHiveValue = toHiveEquivalent(expectedTrinoValue);
        assertThat(actualHiveValue).isEqualTo(expectedHiveValue);
    }

    private static void assertJsonFails(String json)
    {
        assertJsonTrinoFails(json);
        assertJsonHiveFails(json);
    }

    private static void assertJsonTrinoFails(String json)
    {
        assertThatThrownBy(() -> JsonReader.readJson(json, Function.identity()))
                .isInstanceOf(InvalidJsonException.class);
    }

    private static void assertJsonHiveFails(String json)
    {
        assertThatThrownBy(() -> new JSONTokener(false, json).nextValue())
                .isInstanceOf(JSONException.class);
    }

    private static Object unwrapHiveValue(Object value)
    {
        if (value instanceof JSONObject jsonObject) {
            LinkedHashMap<String, Object> unwrapped = new LinkedHashMap<>();
            for (String key : jsonObject.keySet()) {
                unwrapped.put(key, jsonObject.opt(key));
            }
            return unwrapped;
        }
        if (value instanceof JSONArray jsonArray) {
            List<Object> unwrapped = new ArrayList<>();
            for (int i = 0; i < jsonArray.length(); i++) {
                unwrapped.add(jsonArray.opt(i));
            }
            return unwrapped;
        }
        if (value == JSONObject.NULL) {
            return null;
        }
        return value;
    }

    private static Object toHiveEquivalent(Object value)
    {
        if (value == null) {
            return null;
        }
        if (value instanceof Map<?, ?> map) {
            return map.entrySet().stream()
                    .collect(Collectors.toUnmodifiableMap(Entry::getKey, entry -> toHiveEquivalent(entry.getValue())));
        }
        if (value instanceof List<?> list) {
            return list.stream()
                    .map(JsonReaderTest::toHiveEquivalent)
                    .collect(Collectors.toCollection(ArrayList::new));
        }
        if (value instanceof JsonString jsonString) {
            if (jsonString.quoted()) {
                return jsonString.value();
            }

            String string = jsonString.value();

            if (string.equalsIgnoreCase("true")) {
                return true;
            }
            if (string.equalsIgnoreCase("false")) {
                return false;
            }

            try {
                long longValue = Long.parseLong(string);
                if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                    return (int) longValue;
                }
                else {
                    return longValue;
                }
            }
            catch (NumberFormatException ignored) {
            }

            try {
                BigDecimal asDecimal = new BigDecimal(string);
                double asDouble = Double.parseDouble(string);
                return asDecimal.compareTo(BigDecimal.valueOf(asDouble)) == 0 ? asDouble : asDecimal;
            }
            catch (NumberFormatException ignored) {
            }

            return string;
        }
        throw new IllegalArgumentException("Unsupported type: " + value.getClass().getSimpleName());
    }
}
