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

import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.google.inject.Inject;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;

public class SymbolKeyDeserializer
        extends KeyDeserializer
{
    private final TypeManager typeManager;

    @Inject
    public SymbolKeyDeserializer(TypeManager typeManager)
    {
        this.typeManager = typeManager;
    }

    @Override
    public Object deserializeKey(String key, DeserializationContext context)
    {
        String[] parts = key.split("::");
        return new Symbol(typeManager.getType(TypeId.of(parts[1])), parts[0]);
    }
}
