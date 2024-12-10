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
package io.trino.sql.dialect.trino;

import com.google.inject.Inject;
import io.trino.spi.TrinoException;
import io.trino.spi.type.TypeId;
import io.trino.spi.type.TypeManager;
import io.trino.sql.newir.Dialect;
import io.trino.sql.newir.Type;

import java.util.function.Function;

import static io.trino.spi.StandardErrorCode.IR_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class TrinoDialect
        extends Dialect
{
    // dialect name
    public static final String TRINO = "trino";

    private final Function<String, io.trino.spi.type.Type> typeDeserializer;
    private final TrinoAttributeRegistry attributeRegistry;

    @Inject
    public TrinoDialect(TypeManager typeManager, TrinoAttributeRegistry trinoAttributeRegistry)
    {
        super(TRINO);
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(trinoAttributeRegistry, "trinoAttributeRegistry is null");

        this.typeDeserializer = serializedType -> typeManager.getType(TypeId.of(serializedType));
        this.attributeRegistry = trinoAttributeRegistry;
    }

    @Override
    public String formatAttribute(String name, Object attribute)
    {
        return attributeRegistry.getAttributeProperties(name).print(attribute);
    }

    @Override
    public Object parseAttribute(String name, String attribute)
    {
        return attributeRegistry.getAttributeProperties(name).parse(attribute);
    }

    @Override
    public String formatType(Type type)
    {
        return trinoType(type).getTypeId().getId();
    }

    @Override
    public Type parseType(String type)
    {
        return irType(typeDeserializer.apply(type));
    }

    public static io.trino.spi.type.Type trinoType(Type type)
    {
        if (!type.dialect().equals(TRINO)) {
            throw new TrinoException(IR_ERROR, format("expected a type of the %s dialect, actual dialect: %s", TRINO, type.dialect()));
        }

        if (type.dialectType() instanceof io.trino.spi.type.Type trinoType) {
            return trinoType;
        }

        throw new TrinoException(IR_ERROR, "expected a trino type, actual: " + type.dialectType().getClass().getSimpleName());
    }

    public static Type irType(io.trino.spi.type.Type type)
    {
        return new Type(TRINO, type);
    }
}
