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
package io.trino.plugin.paimon;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.spi.type.Type;
import io.trino.type.TypeDeserializer;
import org.apache.paimon.types.DataTypes;
import org.junit.jupiter.api.Test;

import static io.trino.type.InternalTypeManager.TESTING_TYPE_MANAGER;
import static org.assertj.core.api.Assertions.assertThat;

final class TestPaimonColumnHandle
{
    private final JsonCodec<PaimonColumnHandle> codec;

    public TestPaimonColumnHandle()
    {
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(TESTING_TYPE_MANAGER)));
        codec = new JsonCodecFactory(objectMapperProvider).jsonCodec(PaimonColumnHandle.class);
    }

    @Test
    void testTrinoColumnHandle()
    {
        PaimonColumnHandle expected = PaimonColumnHandle.of("name", DataTypes.STRING(), 0);
        String json = codec.toJson(expected);
        PaimonColumnHandle actual = codec.fromJson(json);

        assertThat(actual).isEqualTo(expected);
    }
}
