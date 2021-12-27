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
package io.trino.plugin.redis.util;

import com.google.common.collect.ImmutableMap;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import io.trino.plugin.base.TypeDeserializer;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;

import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public final class CodecSupplier<T>
        implements Supplier<JsonCodec<T>>
{
    private final JsonCodecFactory codecFactory;
    private final Class<T> clazz;

    public CodecSupplier(Class<T> clazz, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new TypeDeserializer(typeManager::getType)));
        this.codecFactory = new JsonCodecFactory(objectMapperProvider);
        this.clazz = requireNonNull(clazz, "clazz is null");
    }

    @Override
    public JsonCodec<T> get()
    {
        return codecFactory.jsonCodec(clazz);
    }
}
