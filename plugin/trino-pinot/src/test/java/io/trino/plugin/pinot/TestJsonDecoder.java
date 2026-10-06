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
package io.trino.plugin.pinot;

import io.trino.json.Json;
import io.trino.json.JsonBlockBuilder;
import io.trino.json.JsonItemEncoding.TypeTag;
import io.trino.plugin.pinot.decoders.JsonDecoder;
import org.junit.jupiter.api.Test;

import static io.trino.type.JsonType.JSON;
import static org.assertj.core.api.Assertions.assertThat;

class TestJsonDecoder
{
    @Test
    void testPreservesMembersAndScalarTypes()
    {
        JsonDecoder decoder = new JsonDecoder(JSON);
        JsonBlockBuilder builder = JSON.createBlockBuilder(null, 2);
        decoder.decode(() -> "{\"b\":1.20,\"a\":1,\"a\":2}", builder);
        decoder.decode(() -> null, builder);
        assertThat(JSON.getObjectValue(builder.buildValueBlock(), 0)).isEqualTo("{\"b\":1.20,\"a\":1,\"a\":2}");
        Json value = (Json) JSON.getObject(builder.buildValueBlock(), 0);
        assertThat(value.objectSize()).isEqualTo(3);
        assertThat(value.objectMember("b").orElseThrow().scalarType()).isEqualTo(TypeTag.DECIMAL);
        assertThat(builder.buildValueBlock().isNull(1)).isTrue();
    }
}
