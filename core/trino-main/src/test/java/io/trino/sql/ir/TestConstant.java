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
package io.trino.sql.ir;

import io.trino.json.Json;
import io.trino.spi.type.ArrayType;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.json.JsonItemBuilder.JSON_ERROR;
import static io.trino.json.JsonItems.fromText;
import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

class TestConstant
{
    @Test
    void testExactJsonIdentity()
    {
        List<Constant> constants = List.of(
                new Constant(JSON, fromText(utf8Slice("1"))),
                new Constant(JSON, fromText(utf8Slice("1.0"))),
                new Constant(JSON, fromText(utf8Slice("1e0"))),
                new Constant(JSON, Json.unchecked(utf8Slice("1"))),
                new Constant(JSON, Json.unchecked(utf8Slice(" 1"))));
        assertThat(new HashSet<>(constants)).hasSize(constants.size());
        Constant copy = new Constant(JSON, fromText(utf8Slice("1")));
        assertThat(copy).isEqualTo(constants.getFirst());
        assertThat(copy.hashCode()).isEqualTo(constants.getFirst().hashCode());
    }

    @Test
    void testErrorDiagnostic()
    {
        Constant error = new Constant(JSON, JSON_ERROR);
        assertThat(error.toString()).contains("Json[ERROR]");
        assertThat(ExpressionFormatter.formatExpression(error)).contains("Json[ERROR]");
        assertThat(new Constant(new ArrayType(JSON), error.getValueAsBlock()).toString()).contains("Json[ERROR]");
    }
}
