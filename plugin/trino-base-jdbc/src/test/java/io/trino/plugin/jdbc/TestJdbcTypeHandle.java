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
package io.trino.plugin.jdbc;

import io.airlift.json.JsonCodec;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.json.JsonCodec.mapJsonCodec;
import static org.assertj.core.api.Assertions.assertThat;

public class TestJdbcTypeHandle
{
    @Test
    public void testJsonSerialization()
    {
        JdbcTypeHandle input = new JdbcTypeHandle(
                10,
                Optional.of("jdbcTypeName"),
                Optional.of(10),
                Optional.of(10),
                Optional.of(10),
                Optional.of(CaseSensitivity.CASE_SENSITIVE));

        JsonCodec<JdbcTypeHandle> codec = jsonCodec(JdbcTypeHandle.class);
        String json = codec.toJson(input);
        assertThat(mapJsonCodec(String.class, Object.class).fromJson(json).keySet())
                .containsExactlyInAnyOrder(
                        "jdbcType",
                        "jdbcTypeName",
                        "columnSize",
                        "decimalDigits",
                        "arrayDimensions",
                        "caseSensitivity");
        assertThat(codec.fromJson(json)).isEqualTo(input);
    }
}
