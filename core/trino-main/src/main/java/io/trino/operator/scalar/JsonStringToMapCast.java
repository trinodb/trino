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
package io.trino.operator.scalar;

import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.TypeSignature;

import static io.trino.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static io.trino.spi.type.TypeSignature.mapType;
import static io.trino.spi.type.VarcharType.VARCHAR;

public final class JsonStringToMapCast
        extends SqlScalarFunction
{
    public static final JsonStringToMapCast JSON_STRING_TO_MAP = new JsonStringToMapCast();
    public static final String JSON_STRING_TO_MAP_NAME = "$internal$json_string_to_map_cast";

    private JsonStringToMapCast()
    {
        super(FunctionMetadata.scalarBuilder()
                .signature(Signature.builder()
                        .name(JSON_STRING_TO_MAP_NAME)
                        .comparableTypeParameter("K")
                        .typeVariable("V")
                        .returnType(mapType(new TypeSignature("K"), new TypeSignature("V")))
                        .argumentType(VARCHAR)
                        .build())
                .nullable()
                .hidden()
                .noDescription()
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        return JSON_TO_MAP.specialize(boundSignature);
    }
}
