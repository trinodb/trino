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

import com.fasterxml.jackson.core.exc.StreamConstraintsException;
import io.trino.json.Json;
import io.trino.json.JsonArray;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.json.TypedValue;
import io.trino.operator.scalar.json.JsonOutputConversionException;
import io.trino.operator.scalar.json.JsonOutputFunctions;
import io.trino.spi.type.SqlVarbinary;
import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.parallel.Execution;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_ARRAY;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.EMPTY_OBJECT;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.ERROR;
import static io.trino.sql.tree.JsonQuery.EmptyOrErrorBehavior.NULL;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;
import static org.junit.jupiter.api.parallel.ExecutionMode.CONCURRENT;

@TestInstance(PER_CLASS)
@Execution(CONCURRENT)
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
    public void testOutputBeyondNestingLimit()
    {
        Json tree = JsonItemBuilder.JSON_NULL;
        for (int i = 0; i < 1025; i++) {
            tree = new JsonArray(List.of(tree));
        }
        Json json = tree;
        assertThat(JsonOutputFunctions.jsonToVarchar(json, NULL.ordinal(), false)).isNull();
        assertThat(JsonOutputFunctions.jsonToVarbinaryUtf32(json, NULL.ordinal(), false)).isNull();
        assertThatThrownBy(() -> JsonOutputFunctions.jsonToVarchar(json, ERROR.ordinal(), false))
                .isInstanceOf(JsonOutputConversionException.class)
                .rootCause()
                .isInstanceOf(StreamConstraintsException.class)
                .hasMessageContaining("1024");
        assertThatThrownBy(() -> JsonOutputFunctions.jsonToVarbinaryUtf32(json, ERROR.ordinal(), false))
                .isInstanceOf(JsonOutputConversionException.class)
                .rootCause()
                .isInstanceOf(StreamConstraintsException.class)
                .hasMessageContaining("1024");
    }

    @Test
    public void testJsonOutputPreservesScalarTypes()
    {
        Json result = JsonOutputFunctions.jsonToJson(JsonItemBuilder.encodeDouble(1.0), ERROR.ordinal(), false);
        assertThat(result.scalarType()).isEqualTo(TypeTag.DOUBLE);
        assertThat(result.materializeScalar().type()).isEqualTo(DOUBLE);
        assertThat(result.materializeScalar().getDoubleValue()).isEqualTo(1.0);
        Json decimal = JsonOutputFunctions.jsonToJson(new TypedValue(createDecimalType(4, 2), 100L), ERROR.ordinal(), false);
        assertThat(decimal.materializeScalar().type()).isEqualTo(createDecimalType(4, 2));
        assertThat(decimal.materializeScalar().getLongValue()).isEqualTo(100);
    }

    @Test
    public void testJsonOutputErrorBehavior()
    {
        assertThat(JsonOutputFunctions.jsonToJson(JsonItemBuilder.JSON_ERROR, NULL.ordinal(), false)).isNull();
        assertThatThrownBy(() -> JsonOutputFunctions.jsonToJson(JsonItemBuilder.JSON_ERROR, ERROR.ordinal(), false))
                .isInstanceOf(JsonOutputConversionException.class);
        Json array = JsonOutputFunctions.jsonToJson(JsonItemBuilder.JSON_ERROR, EMPTY_ARRAY.ordinal(), false);
        assertThat(array.isArray()).isTrue();
        assertThat(array.arraySize()).isZero();
        Json object = JsonOutputFunctions.jsonToJson(JsonItemBuilder.JSON_ERROR, EMPTY_OBJECT.ordinal(), false);
        assertThat(object.isObject()).isTrue();
        assertThat(object.objectSize()).isZero();
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
                .isEqualTo(new SqlVarbinary(OUTPUT.getBytes(StandardCharsets.UTF_32LE)));
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
