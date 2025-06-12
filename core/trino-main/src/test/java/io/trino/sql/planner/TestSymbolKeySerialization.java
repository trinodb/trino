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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class TestSymbolKeySerialization
{
    private static final TestingTypeManager TYPE_MANAGER = new TestingTypeManager();
    private static final ObjectMapperProvider OBJECT_MAPPER_PROVIDER = createObjectMapperProvider(TYPE_MANAGER);
    private static final JsonCodec<Map<Symbol, String>> SYMBOL_KEY_CODEC = new JsonCodecFactory(OBJECT_MAPPER_PROVIDER)
            .mapJsonCodec(Symbol.class, String.class);

    @Test
    void testRoundTrip()
    {
        Map<Symbol, String> symbols = Map.of(
                new Symbol(TYPE_MANAGER.getType(TypeId.of("integer")), "a"), "value",
                new Symbol(TYPE_MANAGER.getType(TypeId.of("varchar")), "b"), "value2");

        assertThat(SYMBOL_KEY_CODEC.fromJson(SYMBOL_KEY_CODEC.toJson(symbols)))
                .isEqualTo(symbols);
    }

    private static ObjectMapperProvider createObjectMapperProvider(TestingTypeManager typeManager)
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.of(Symbol.class, new SymbolKeyDeserializer(typeManager)));
        provider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager::getType)));
        return provider;
    }
}
