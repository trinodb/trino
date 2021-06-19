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

import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import io.trino.metadata.FunctionBinding;
import io.trino.metadata.SqlOperator;
import io.trino.spi.block.Block;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeSignature;
import io.trino.util.JsonUtil.JsonGeneratorWriter;

import java.io.IOException;
import java.lang.invoke.MethodHandle;

import static com.google.common.base.Preconditions.checkArgument;
import static io.trino.metadata.Signature.castableToTypeParameter;
import static io.trino.operator.scalar.JsonOperators.JSON_FACTORY;
import static io.trino.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.type.TypeSignature.arrayType;
import static io.trino.type.JsonType.JSON;
import static io.trino.util.Failures.checkCondition;
import static io.trino.util.JsonUtil.canCastToJson;
import static io.trino.util.JsonUtil.createJsonGenerator;
import static io.trino.util.Reflection.methodHandle;

public class ArrayToJsonCast
        extends SqlOperator
{
    public static final ArrayToJsonCast ARRAY_TO_JSON = new ArrayToJsonCast(false);
    public static final ArrayToJsonCast LEGACY_ARRAY_TO_JSON = new ArrayToJsonCast(true);

    private static final MethodHandle METHOD_HANDLE = methodHandle(ArrayToJsonCast.class, "toJson", JsonGeneratorWriter.class, Block.class);

    private final boolean legacyRowToJson;

    private ArrayToJsonCast(boolean legacyRowToJson)
    {
        super(OperatorType.CAST,
                ImmutableList.of(castableToTypeParameter("T", JSON.getTypeSignature())),
                ImmutableList.of(),
                JSON.getTypeSignature(),
                ImmutableList.of(arrayType(new TypeSignature("T"))),
                false);
        this.legacyRowToJson = legacyRowToJson;
    }

    @Override
    protected ScalarFunctionImplementation specialize(FunctionBinding functionBinding)
    {
        checkArgument(functionBinding.getArity() == 1, "Expected arity to be 1");
        Type type = functionBinding.getTypeVariable("T");
        Type arrayType = functionBinding.getBoundSignature().getArgumentTypes().get(0);
        checkCondition(canCastToJson(arrayType), INVALID_CAST_ARGUMENT, "Cannot cast %s to JSON", arrayType);

        JsonGeneratorWriter writer = JsonGeneratorWriter.createJsonGeneratorWriter(type, legacyRowToJson);
        MethodHandle methodHandle = METHOD_HANDLE.bindTo(writer);
        return new ChoicesScalarFunctionImplementation(
                functionBinding,
                FAIL_ON_NULL,
                ImmutableList.of(NEVER_NULL),
                methodHandle);
    }

    public static Slice toJson(JsonGeneratorWriter writer, Block block)
    {
        try {
            SliceOutput output = new DynamicSliceOutput(40);
            try (JsonGenerator jsonGenerator = createJsonGenerator(JSON_FACTORY, output)) {
                jsonGenerator.writeStartArray();
                for (int i = 0; i < block.getPositionCount(); i++) {
                    writer.writeJsonValue(jsonGenerator, block, i);
                }
                jsonGenerator.writeEndArray();
            }
            return output.slice();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
