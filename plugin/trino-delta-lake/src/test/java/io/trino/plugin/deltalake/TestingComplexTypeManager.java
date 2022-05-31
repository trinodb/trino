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
package io.trino.plugin.deltalake;

import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TestingTypeManager;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeSignature;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class TestingComplexTypeManager
        extends TestingTypeManager
{
    private final TypeOperators typeOperators = new TypeOperators();

    @Override
    public Type getType(TypeSignature signature)
    {
        switch (signature.getBase()) {
            case StandardTypes.MAP:
                return new MapType(
                        getType(signature.getTypeParametersAsTypeSignatures().get(0)),
                        getType(signature.getTypeParametersAsTypeSignatures().get(1)),
                        typeOperators);
            case StandardTypes.ARRAY:
                return new ArrayType(getType(signature.getTypeParametersAsTypeSignatures().get(0)));
            case StandardTypes.ROW:
                return RowType.from(signature.getParameters().stream()
                        .map(namedType -> new RowType.Field(namedType.getNamedTypeSignature().getName(), getType(namedType.getNamedTypeSignature().getTypeSignature())))
                        .collect(toImmutableList()));
            default:
                return super.getType(signature);
        }
    }
}
