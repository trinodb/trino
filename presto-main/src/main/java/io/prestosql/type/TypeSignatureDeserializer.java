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
import com.google.common.collect.ImmutableSet;
import io.prestosql.spi.type.TypeSignature;

import javax.inject.Inject;

import static io.prestosql.operator.TypeSignatureParser.parseTypeSignature;

public final class TypeSignatureDeserializer
        extends FromStringDeserializer<TypeSignature>
{
    @Inject
    public TypeSignatureDeserializer()
    {
        super(TypeSignature.class);
    }

    @Override
    protected TypeSignature _deserialize(String value, DeserializationContext context)
    {
        return parseTypeSignature(value, ImmutableSet.of());
    }
}
