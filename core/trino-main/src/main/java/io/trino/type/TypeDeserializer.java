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
package io.prestosql.type;

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.FromStringDeserializer;
import io.prestosql.metadata.Metadata;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeId;

import javax.inject.Inject;

import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public final class TypeDeserializer
        extends FromStringDeserializer<Type>
{
    private final Function<TypeId, Type> typeLoader;

    @Inject
    public TypeDeserializer(Metadata metadata)
    {
        this(requireNonNull(metadata, "metadata is null")::getType);
    }

    public TypeDeserializer(Function<TypeId, Type> typeLoader)
    {
        super(Type.class);
        this.typeLoader = requireNonNull(typeLoader, "typeLoader is null");
    }

    @Override
    protected Type _deserialize(String value, DeserializationContext context)
    {
        return typeLoader.apply(TypeId.of(value));
    }
}
