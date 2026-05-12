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
import io.trino.spi.type.ArrayType;
import io.trino.util.JsonUtil.BlockBuilderAppender;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.numericVariable;
import static io.trino.spi.type.TypeTemplates.type;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.canCastFromJson;

public final class JsonStringToArrayCast
        extends SqlScalarFunction
{
    public static final JsonStringToArrayCast JSON_STRING_TO_ARRAY = new JsonStringToArrayCast();
    public static final String JSON_STRING_TO_ARRAY_NAME = "$internal$json_string_to_array_cast";

    private JsonStringToArrayCast()
    {
        super(FunctionMetadata.scalarBuilder(JSON_STRING_TO_ARRAY_NAME)
                .signature(Signature.builder()
                        .typeVariable("T")
                        .numericVariable("N")
                        .returnType(arrayType(typeVariable("T")))
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
        ArrayType arrayType = (ArrayType) boundSignature.getReturnType();
        checkCondition(canCastFromJson(arrayType), INVALID_CAST_ARGUMENT, "Cannot cast JSON to %s", arrayType);

        BlockBuilderAppender arrayAppender = BlockBuilderAppender.createBlockBuilderAppender(arrayType);
        MethodHandle methodHandle = JsonToArrayCast.TEXT_METHOD_HANDLE.bindTo(arrayType).bindTo(arrayAppender);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                NULLABLE_RETURN,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }
}
