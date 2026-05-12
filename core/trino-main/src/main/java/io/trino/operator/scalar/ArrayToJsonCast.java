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
import io.trino.json.Json;
import io.trino.json.JsonItemBuilder;
import io.trino.json.JsonNestingDepthException;
import io.trino.metadata.SqlScalarFunction;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionDependencies;
import io.trino.spi.function.FunctionMetadata;
import io.trino.spi.function.Signature;
import io.trino.spi.type.ArrayType;
import io.trino.util.JsonUtil.JsonValueWriter;

import java.lang.invoke.MethodHandle;

import static io.trino.json.JsonItems.MAX_NESTING_DEPTH;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.OperatorType.CAST;
import static io.trino.spi.type.TypeTemplates.arrayType;
import static io.trino.spi.type.TypeTemplates.typeVariable;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.Reflection.methodHandle;

public class ArrayToJsonCast
        extends SqlScalarFunction
{
    public static final ArrayToJsonCast ARRAY_TO_JSON = new ArrayToJsonCast();

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayToJsonCast.class, "toJson", JsonValueWriter.class, Block.class);

    private ArrayToJsonCast()
    {
        super(FunctionMetadata.operatorBuilder(CAST)
                .signature(Signature.builder()
                        .castableToTypeParameter("T", JSON.getTypeDescriptor())
                        .returnType(JSON)
                        .argumentType(arrayType(typeVariable("T")))
                        .build())
                .build());
    }

    @Override
    public SpecializedSqlScalarFunction specialize(BoundSignature boundSignature, FunctionDependencies functionDependencies)
    {
        ArrayType arrayType = (ArrayType) boundSignature.getArgumentTypes().get(0);
        checkCondition(canCastToJson(arrayType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", arrayType);

        JsonValueWriter writer = JsonValueWriter.createJsonValueWriter(arrayType.getElementType());
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(writer);
        return new ChoicesSpecializedSqlScalarFunction(
                boundSignature,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    public static Json toJson(JsonValueWriter writer, Block block)
    {
        try {
            return JsonItemBuilder.encodeWithDepthLimit(jsonWriter -> {
                jsonWriter.startArray();
                for (int position = 0; position < block.getPositionCount(); position++) {
                    writer.writeJsonValue(jsonWriter, block, position);
                }
                jsonWriter.endArray();
            }, MAX_NESTING_DEPTH);
        }
        catch (JsonNestingDepthException e) {
            throw new TrinoException(INVALID_CAST_ARGUMENT, e.getMessage(), e);
        }
    }
}
