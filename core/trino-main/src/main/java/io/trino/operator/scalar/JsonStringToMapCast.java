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

import com.google.common.collect.ImmutableList;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.MapType;
import io.trino.util.JsonUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeTemplates.mapType;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.canCastFromJson;

public final class JsonStringToMapCast
        extends SqlScalarFunction
{
    public static final JsonStringToMapCast JSON_STRING_TO_MAP = new JsonStringToMapCast();
    public static final String JSON_STRING_TO_MAP_NAME = "$internal$json_string_to_map_cast";

    private JsonStringToMapCast()
    {
        super(FunctionMetadata.scalarBuilder(JSON_STRING_TO_MAP_NAME)
                .signature(Signature.builder()
                        .comparableTypeParameter("K")
                        .typeVariable("V")
                        .numericVariable("N")
                        .returnType(mapType(typeVariable("K"), typeVariable("V")))
                        .argumentType(type("varchar", numericVariable("N")))
                        .build())
                .nullable()
                .hidden()
                .description("")
                .build());
    }

    @Override
    protected SpecializedSqlScalarFunction specialize(BoundSignature boundSignature)
    {
        MapType mapType = (MapType) boundSignature.getReturnType();
        checkCondition(canCastFromJson(mapType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", mapType);

        BlockBuilderAppender mapAppender = BlockBuilderAppender.createBlockBuilderAppender(mapType);
        MethodHandle methodHandle = JsonToMapCast.TEXT_METHOD_HANDLE.bindTo(mapType).bindTo(mapAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }
}
