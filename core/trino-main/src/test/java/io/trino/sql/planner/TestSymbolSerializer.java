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

import com.fasterxml.jackson.databind.KeyDeserializer;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.RowType.field;
import static io.trino.spi.type.RowType.rowType;
import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

class TestSymbolSerializer
{
    private static final JsonCodecFactory CODEC_FACTORY;

    static {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        provider.setKeyDeserializers(ImmutableMap.<Class<?>, KeyDeserializer>builder()
                .put(Symbol.class, new SymbolKeyDeserializer(TESTING_TYPE_MANAGER))
                .buildOrThrow());

        provider.setJsonDeserializers(ImmutableMap.of(
                Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));

        CODEC_FACTORY = new JsonCodecFactory(provider);
    }

    private static final JsonCodec<Symbol> SYMBOL_CODEC = CODEC_FACTORY.jsonCodec(Symbol.class);
    private static final JsonCodec<Map<Symbol, String>> MAP_CODEC = CODEC_FACTORY.jsonCodec(new TypeToken<>() { });

    @Test
    void testAsValue()
    {
        assertThat(SYMBOL_CODEC.fromJson(SYMBOL_CODEC.toJson(new Symbol(BIGINT, "abc123!@# :/{}[]*"))))
                .isEqualTo(new Symbol(BIGINT, "abc123!@# :/{}[]*"));

        assertThat(SYMBOL_CODEC.fromJson(SYMBOL_CODEC.toJson(new Symbol(BIGINT, "abc::def"))))
                .isEqualTo(new Symbol(BIGINT, "abc::def"));

        assertThat(SYMBOL_CODEC.fromJson(SYMBOL_CODEC.toJson(new Symbol(BIGINT, "a:"))))
                .isEqualTo(new Symbol(BIGINT, "a:"));

        assertThat(SYMBOL_CODEC.fromJson(SYMBOL_CODEC.toJson(new Symbol(rowType(field("abc", BIGINT)), "a:"))))
                .isEqualTo(new Symbol(rowType(field("abc", BIGINT)), "a:"));

        assertThat(SYMBOL_CODEC.fromJson(SYMBOL_CODEC.toJson(new Symbol(rowType(field("abc::123", BIGINT)), "a:"))))
                .isEqualTo(new Symbol(rowType(field("abc::123", BIGINT)), "a:"));
    }

    @Test
    void testAsKey()
    {
        assertThat(MAP_CODEC.fromJson(MAP_CODEC.toJson(ImmutableMap.of(new Symbol(BIGINT, "abc123!@# :/{}[]*"), ""))))
                .isEqualTo(ImmutableMap.of(new Symbol(BIGINT, "abc123!@# :/{}[]*"), ""));

        assertThat(MAP_CODEC.fromJson(MAP_CODEC.toJson(ImmutableMap.of(new Symbol(BIGINT, "a:"), ""))))
                .isEqualTo(ImmutableMap.of(new Symbol(BIGINT, "a:"), ""));

        assertThat(MAP_CODEC.fromJson(MAP_CODEC.toJson(ImmutableMap.of(new Symbol(rowType(field("abc::123", BIGINT)), "a:"), ""))))
                .isEqualTo(ImmutableMap.of(new Symbol(rowType(field("abc::123", BIGINT)), "a:"), ""));
    }
}
