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
package io.trino.type;

import io.trino.FeaturesConfig;
import io.trino.metadata.TypeRegistry;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import javax.inject.Inject;

public final class InternalTypeManager
        implements TypeManager
{
    public static final TypeManager TESTING_TYPE_MANAGER = new InternalTypeManager(new TypeRegistry(new TypeOperators(), new FeaturesConfig()));

    private final TypeRegistry typeRegistry;

    @Inject
    public InternalTypeManager(TypeRegistry typeRegistry)
    {
        this.typeRegistry = typeRegistry;
    }

    @Override
    public Type getType(TypeSignature signature)
    {
        return typeRegistry.getType(signature);
    }

    @Override
    public Type fromSqlType(String type)
    {
        return typeRegistry.fromSqlType(type);
    }

    @Override
    public Type getType(TypeId id)
    {
        return typeRegistry.getType(id);
    }

    @Override
    public TypeOperators getTypeOperators()
    {
        return typeRegistry.getTypeOperators();
    }
}
