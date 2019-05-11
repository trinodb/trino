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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionMetadata;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.metadata.SqlScalarFunction;
import io.prestosql.spi.type.TypeSignature;

import static io.prestosql.metadata.FunctionKind.SCALAR;
import static io.prestosql.metadata.Signature.comparableTypeParameter;
import static io.prestosql.metadata.Signature.typeVariable;
import static io.prestosql.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static io.prestosql.spi.type.TypeSignature.mapType;
import static io.prestosql.spi.type.VarcharType.VARCHAR;

public final class JsonStringToMapCast
        extends SqlScalarFunction
{
    public static final JsonStringToMapCast JSON_STRING_TO_MAP = new JsonStringToMapCast();
    public static final String JSON_STRING_TO_MAP_NAME = "$internal$json_string_to_map_cast";

    private JsonStringToMapCast()
    {
        super(new FunctionMetadata(
                new Signature(
                        JSON_STRING_TO_MAP_NAME,
                        SCALAR,
                        ImmutableList.of(comparableTypeParameter("K"), typeVariable("V")),
                        ImmutableList.of(),
                        mapType(new TypeSignature("K"), new TypeSignature("V")),
                        ImmutableList.of(VARCHAR.getTypeSignature()),
                        false),
                true,
                true,
                ""));
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, Metadata metadata)
    {
        return JSON_TO_MAP.specialize(boundVariables, arity, metadata);
    }
}
